package org.example.puffin;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.util.List;

import static org.example.puffin.PuffinBuilder.objectMapper;

public class Blob {
    @JsonProperty("type")
    String type;
    @JsonProperty("fields")
    List<Integer> fields;

    @JsonIgnoreProperties
    byte[] content;
    @JsonProperty("length")
    long length;
    @JsonProperty("offset")
    long offset;

    public Blob() {
    }

    public Blob(String type, List<Integer> fields, byte[] content, long offset) {
        this.type = type;
        this.fields = fields;
        this.content = content;
        this.offset = offset;
        this.length = content.length;
    }

    public byte[] content() {
        return content;
    }

    public String toJson() throws JsonProcessingException {
        return objectMapper.writeValueAsString(this);
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setLength(long length) {
        this.length = length;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public void setFields(List<Integer> fields) {
        this.fields = fields;
    }
}
