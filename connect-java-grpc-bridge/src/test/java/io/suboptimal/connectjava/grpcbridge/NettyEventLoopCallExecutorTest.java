package io.suboptimal.connectjava.grpcbridge;

import io.netty.util.concurrent.ThreadAwareExecutor;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.RejectedExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NettyEventLoopCallExecutorTest {
    final TestThreadAwareExecutor testExecutor = new TestThreadAwareExecutor();
    final NettyEventLoopCallExecutor callExecutor =
        new NettyEventLoopCallExecutor(testExecutor);

    /** Order of execution, which is the property the latch exists to protect. */
    final List<String> executed = new ArrayList<>();

    Runnable command(String name) {
        return () -> executed.add(name);
    }

    @Test
    void executesTaskImmediatelyOnSameThread() {
        testExecutor.isExecutorThread = true;

        callExecutor.execute(command("inline"));

        assertThat(executed).containsExactly("inline");
        assertThat(testExecutor.pendingCommands).isEmpty();
    }

    @Test
    void schedulesExecutionOnDifferentThread() {
        testExecutor.isExecutorThread = false;

        callExecutor.execute(command("queued"));

        assertThat(executed).isEmpty();
        assertThat(testExecutor.pendingCommands).hasSize(1);

        testExecutor.runAllPendingTasks();

        assertThat(executed).containsExactly("queued");
        assertThat(testExecutor.pendingCommands).isEmpty();
    }

    @Test
    void schedulesExecutionForCrossThreadScenario() {
        // A service that calls ServerCall from its own thread and then continues on the event
        // loop: running the second task inline would let it overtake the first.
        testExecutor.isExecutorThread = false;
        callExecutor.execute(command("off-loop"));

        testExecutor.isExecutorThread = true;
        callExecutor.execute(command("on-loop"));

        assertThat(executed).isEmpty();
        assertThat(testExecutor.pendingCommands).hasSize(2);

        testExecutor.runAllPendingTasks();

        assertThat(executed).containsExactly("off-loop", "on-loop");
    }

    @Test
    void returnsToInlineExecutionOnceTheQueueDrains() {
        testExecutor.isExecutorThread = false;
        callExecutor.execute(command("queued"));
        testExecutor.runAllPendingTasks();

        // The latch has to release, or the inline fast path is lost for the rest of the call.
        testExecutor.isExecutorThread = true;
        callExecutor.execute(command("inline"));

        assertThat(executed).containsExactly("queued", "inline");
        assertThat(testExecutor.pendingCommands).isEmpty();
    }

    @Test
    void restoresTheInlinePathAfterTheExecutorRejectsSubmission() {
        testExecutor.isExecutorThread = false;
        testExecutor.rejecting = true;

        assertThatThrownBy(() -> callExecutor.execute(command("rejected")))
            .isInstanceOf(RejectedExecutionException.class);

        // A rejected submission increments the latch but never runs, so nothing would ever
        // decrement it again: without the rollback everything below would queue forever.
        testExecutor.rejecting = false;
        testExecutor.isExecutorThread = true;
        callExecutor.execute(command("inline"));

        assertThat(executed).containsExactly("inline");
        assertThat(testExecutor.pendingCommands).isEmpty();
    }

    @Test
    void runsSubmissionMadeFromInsideQueuedTaskNested() {
        // Must nest even though the enclosing task was queued, and therefore still counted by
        // the latch. Deferring here would make nesting depend on how the enclosing task happened
        // to be dispatched.
        testExecutor.isExecutorThread = false;
        callExecutor.execute(() -> {
            executed.add("outer-start");
            callExecutor.execute(command("nested"));
            executed.add("outer-end");
        });

        testExecutor.runAllPendingTasks();

        assertThat(executed).containsExactly("outer-start", "nested", "outer-end");
    }

    @Test
    void nestedSubmissionDoesNotOvertakeAlreadyQueuedTasks() {
        testExecutor.isExecutorThread = false;
        callExecutor.execute(() -> {
            executed.add("first-start");
            callExecutor.execute(command("nested"));
            executed.add("first-end");
        });
        callExecutor.execute(command("second"));

        testExecutor.runAllPendingTasks();

        // The nested task is a continuation of the first one, so it runs within it - but the
        // task queued behind the first one still goes last.
        assertThat(executed).containsExactly("first-start", "nested", "first-end", "second");
        assertThat(testExecutor.pendingCommands).isEmpty();
    }

    @Test
    void runsSubmissionMadeFromInsideInlineTaskNested() {
        // The mainstream path: an inbound event runs inline on the event loop and the service
        // closes the call from within the listener callback. Must agree with the queued case.
        testExecutor.isExecutorThread = true;
        callExecutor.execute(() -> {
            executed.add("outer-start");
            callExecutor.execute(command("nested"));
            executed.add("outer-end");
        });

        assertThat(executed).containsExactly("outer-start", "nested", "outer-end");
        assertThat(testExecutor.pendingCommands).isEmpty();
    }

    static class TestThreadAwareExecutor implements ThreadAwareExecutor {
        final Queue<Runnable> pendingCommands = new LinkedList<>();
        boolean isExecutorThread;
        boolean rejecting;

        void runAllPendingTasks() {
            boolean previous = isExecutorThread;
            isExecutorThread = true;   // draining happens on the executor's own thread
            try {
                Runnable command;
                while ((command = pendingCommands.poll()) != null) {
                    command.run();
                }
            } finally {
                isExecutorThread = previous;
            }
        }

        @Override
        public boolean isExecutorThread(Thread ignore) {
            return isExecutorThread;
        }

        @Override
        public void execute(Runnable command) {
            if (rejecting) {
                throw new RejectedExecutionException("test executor is rejecting");
            }
            pendingCommands.add(command);
        }
    }
}
