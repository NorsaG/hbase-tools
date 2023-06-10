package org.evla.hbase.configuration;

import java.util.Objects;

public class HBaseToolsProperty {
    private final String name;
    private final String defaultValue;
    private final String description;

    private String value;

    public HBaseToolsProperty(String name, String defaultValue, String description) {
        this.name = name;
        this.defaultValue = defaultValue;
        this.description = description;

        this.value = defaultValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HBaseToolsProperty  that = (HBaseToolsProperty ) o;
        return Objects.equals(name, that.name) && Objects.equals(defaultValue, that.defaultValue) && Objects.equals(description, that.description) && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, defaultValue, description, value);
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public String getDescription() {
        return description;
    }

    public String getValue() {
        return value;
    }
}

