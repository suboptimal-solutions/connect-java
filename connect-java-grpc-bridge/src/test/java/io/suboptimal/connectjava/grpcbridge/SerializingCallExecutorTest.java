package io.suboptimal.connectjava.grpcbridge;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

class SerializingCallExecutorTest {
    final RejectingManualExecutor testExecutor = new RejectingManualExecutor();
    final SerializingCallExecutor callExecutor = new SerializingCallExecutor(testExecutor);

    /** Order of execution, which is the property the latch and the queue exist to protect. */
    final List<String> executed = new ArrayList<>();

    Runnable command(String name) {
        return () -> executed.add(name);
    }

    @Test
    void neverClaimsToRunOnTheEventLoop() {
        // The delegate is an arbitrary application executor, and a wrong true here mixes inline
        // writes with posted ones and reorders the response.
        assertThat(callExecutor.runsOnEventLoop()).isFalse();
    }

    @Test
    void queuesSubmissionsBehindASingleDrain() {
        callExecutor.execute(command("first"));
        callExecutor.execute(command("second"));
        callExecutor.execute(command("third"));

        // One drain serves all three: the latch keeps the later submissions from scheduling
        // another one, which is what stops two drains from running at once.
        assertThat(executed).isEmpty();
        assertThat(testExecutor.getSize()).isEqualTo(1);

        testExecutor.runAllTasks();

        assertThat(executed).containsExactly("first", "second", "third");
        assertThat(testExecutor.isEmpty()).isTrue();
    }

    @Test
    void schedulesAgainOnceTheDrainFinished() {
        callExecutor.execute(command("first"));
        testExecutor.runAllTasks();

        // The latch has to release, or nothing would ever drain the queue again.
        callExecutor.execute(command("second"));

        assertThat(testExecutor.getSize()).isEqualTo(1);
        testExecutor.runAllTasks();
        assertThat(executed).containsExactly("first", "second");
    }

    @Test
    void runsSubmissionMadeFromInsideRunningTaskNested() {
        // Required rather than optimal: a service that closes the call from a listener callback and
        // then throws has to see its own status reach the client, and a deferred close loses that
        // race to the INTERNAL the bridge reports for the escaped throwable.
        callExecutor.execute(() -> {
            executed.add("outer-start");
            callExecutor.execute(command("nested"));
            executed.add("outer-end");
        });

        testExecutor.runAllTasks();

        assertThat(executed).containsExactly("outer-start", "nested", "outer-end");
    }

    @Test
    void nestedSubmissionDoesNotOvertakeAlreadyQueuedTasks() {
        callExecutor.execute(() -> {
            executed.add("first-start");
            callExecutor.execute(command("nested"));
            executed.add("first-end");
        });
        callExecutor.execute(command("second"));

        testExecutor.runAllTasks();

        // The nested task is a continuation of the first one, so it runs within it - but the task
        // queued behind the first one still goes last.
        assertThat(executed).containsExactly("first-start", "nested", "first-end", "second");
    }

    @Test
    void submissionFromAnotherThreadIsQueuedRatherThanNested() {
        // The nesting check compares thread identity, not "is a drain running". Were it the
        // latter, this task would run in the foreign thread's stack while the drain is still
        // inside the enclosing one, putting two tasks of the same call in flight at once.
        //
        // The comparison is also the direction the benign race on runningThread is safe in: the
        // foreign thread may read a stale value, but a stale value is never its own identity.
        callExecutor.execute(() -> {
            executed.add("outer-start");
            Thread foreign = new Thread(() -> callExecutor.execute(command("foreign")), "foreign");
            foreign.start();
            // Joined rather than left to race: the submission has to land while this drain is
            // still running, or schedule() would win the latch and queue a drain nobody runs.
            join(foreign);
            executed.add("outer-end");
        });

        testExecutor.runAllTasks();

        assertThat(executed).containsExactly("outer-start", "outer-end", "foreign");
        assertThat(testExecutor.isEmpty()).isTrue();
    }

    @Test
    void exceptionFromAQueuedTaskDoesNotStopTheDrain() {
        callExecutor.execute(() -> {
            throw new IllegalStateException("boom");
        });
        callExecutor.execute(command("after"));

        testExecutor.runAllTasks();

        // Nowhere to propagate to - the drain runs on the delegate's thread, not the submitter's -
        // so it is logged and the rest of the queue still runs. gRPC's SerializingExecutor is the
        // same.
        assertThat(executed).containsExactly("after");
        assertThat(testExecutor.isEmpty()).isTrue();
    }

    @Test
    void exceptionFromANestedTaskReachesItsCaller() {
        // The other half of running nested tasks inline, and a deliberate difference from the
        // queued case above: the throw lands in the caller's stack, which is what lets
        // GrpcBridgeHandler's catch around a listener callback turn it into failInternal.
        AtomicReference<Throwable> caught = new AtomicReference<>();
        callExecutor.execute(() -> {
            executed.add("outer-start");
            caught.set(catchThrowable(() -> callExecutor.execute(() -> {
                throw new IllegalStateException("nested boom");
            })));
            executed.add("outer-end");
        });

        testExecutor.runAllTasks();

        assertThat(caught.get())
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("nested boom");
        assertThat(executed).containsExactly("outer-start", "outer-end");
    }

    @Test
    void releasesTheLatchAfterTheDelegateRejectsSubmission() {
        testExecutor.rejecting = true;

        assertThatThrownBy(() -> callExecutor.execute(command("rejected")))
            .isInstanceOf(RejectedExecutionException.class);

        // A rejected submission raises the latch but never runs, so nothing would lower it again:
        // without the rollback every later submission would lose the CAS and queue forever.
        testExecutor.rejecting = false;
        callExecutor.execute(command("after"));

        assertThat(testExecutor.getSize()).isEqualTo(1);
        testExecutor.runAllTasks();

        // The rejected command is dropped rather than left to run later behind a task the caller
        // was already told did not make it.
        assertThat(executed).containsExactly("after");
    }

    static void join(Thread thread) {
        try {
            thread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    static class RejectingManualExecutor extends ManualExecutor {
        boolean rejecting;

        @Override
        public void execute(Runnable command) {
            if (rejecting) {
                throw new RejectedExecutionException("test executor is rejecting");
            } else {
                super.execute(command);
            }
        }
    }
}
