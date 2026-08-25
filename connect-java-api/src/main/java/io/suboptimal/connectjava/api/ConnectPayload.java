package io.suboptimal.connectjava.api;

/**
 * Application payload moving between Connect protocol handlers and the terminal handler.
 *
 * @param data decoded request data inbound, or service response data outbound
 */
public record ConnectPayload(Object data) implements ConnectMessage {}
