package io.suboptimal.connectjava.model;

/**
 * Describes a single Connect method.
 *
 * @param methodName      exact method name used for routing
 * @param type            streaming cardinality of the method
 * @param requestType     expected Java type of each decoded inbound payload
 * @param responseType    Java type of each outbound response frame
 * @param idempotent      whether Unary GET is allowed for this method
 */
public record ConnectMethodDefinition(
    String methodName,
    ConnectMethodType type,
    Class<?> requestType,
    Class<?> responseType,
    boolean idempotent)
{}
