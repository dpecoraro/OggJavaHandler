package com.santander.goldengate.helpers;

import oracle.goldengate.datasource.DsOperation;

/*
 * Map GoldenGate operation enum/name to CDC short codes (PT/UP/DL/RR)
 */
public class EntityTypeFormatHandler {

    public String mapEntTyp(DsOperation operation) {
        if (operation == null || operation.getOperationType() == null) {
            return "UN";
        }
        String name = operation.getOperationType().name();
        switch (name) {
            // Insert variants
            case "DO_INSERT":
            case "INSERT":
            case "DO_UNIFIED_INSERT_VAL":
                return "PT";
            // Update variants
            case "DO_UPDATE":
            case "UPDATE":
            case "DO_UNIFIED_UPDATE_VAL":
                return "UP";
            // Delete variants
            case "DO_DELETE":
            case "DELETE":
            case "DO_UNIFIED_DELETE_VAL":
                return "DL";
            // Refresh variants
            case "DO_REFRESH":
            case "REFRESH":
            case "DO_UNIFIED_REFRESH_VAL":
                return "RR";
            default:
                // Fallback: coarse detection for unexpected names
                if (name.contains("INSERT")) {
                    return "PT";
                }
                if (name.contains("UPDATE")) {
                    return "UP";
                }
                if (name.contains("DELETE")) {
                    return "DL";
                }
                if (name.contains("REFRESH")) {
                    return "RR";
                }
                return "UN";
        }
    }
}
