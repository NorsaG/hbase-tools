package org.evla.hbase.report;

public class SingleEvent {
    private final EventType eventType;

    private final Object object;
    private final String problem;

    private final Severity severity;

    public SingleEvent(EventType eventType, Object object, String problem, Severity severity) {
        this.eventType = eventType;
        this.object = object;
        this.problem = problem;
        this.severity = severity;
    }

    @Override
    public String toString() {
        return "SingleEvent{" +
                "type=" + eventType +
                ", object='" + object + '\'' +
                ", problem='" + problem + '\'' +
                ", severity=" + severity +
                '}';
    }

    public EventType getType() {
        return eventType;
    }

    public Object getEventObject() {
        return object;
    }

    public String getProblem() {
        return problem;
    }

    public Severity getSeverity() {
        return severity;
    }



}
