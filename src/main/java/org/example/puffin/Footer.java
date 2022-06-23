package org.example.puffin;

import com.fasterxml.jackson.core.JsonProcessingException;

import java.util.ArrayList;
import java.util.List;

import static org.example.puffin.PuffinBuilder.objectMapper;

public class Footer {
    public List<Blob> blobs;

    public Footer(List<Blob> blobs) {
        this.blobs = blobs;
    }

    public Footer() {
    }

    public String toJson() throws JsonProcessingException {
        String jsonString = objectMapper.writeValueAsString(this);
        System.out.println(jsonString);
        return jsonString;
    }

    public void setBlobs(List<Blob> blobs) {
        this.blobs = blobs;
    }
}
