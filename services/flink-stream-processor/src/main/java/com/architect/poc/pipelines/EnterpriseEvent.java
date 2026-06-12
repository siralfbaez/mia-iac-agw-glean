package com.architect.poc.pipelines;

import java.io.Serializable;
import java.util.List;

public class EnterpriseEvent implements Serializable {
    public String sourceSystem;
    public String entityId;
    public String rawPayload;
    public List<String> aclMetadata; //Meters upstream group/profile IDs... Etc
    public List<String> azureAdPermittedGroups; //Populated after Entra ID lookup
    public List<Float> docVector; //Populated after Vertex AI lookup
    public String metadataCategory; //Populated after Vertex AI classification
    public long eventTs;

    public EnterpriseEvent() {}
}