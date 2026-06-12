package io.suboptimal.connectjava.protocol.client;

import io.suboptimal.connectjava.api.ConnectErrorDetail;
import org.jspecify.annotations.Nullable;

import java.util.List;

/**
 * Parsed fields of a Connect unary error body, independent of error code resolution.
 *
 * @param codeName wire name of the error code (e.g. {@code "not_found"}), or {@code null} if absent
 * @param message  human-readable message, or {@code null} if absent
 * @param details  rich error details; empty if the {@code details} array is absent
 */
public record ConnectErrorBody(@Nullable String codeName,
                               @Nullable String message,
                               List<ConnectErrorDetail> details) {}
