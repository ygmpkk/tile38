package com.tile38.model.result;

import lombok.Data;
import lombok.Builder;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Standardized API response wrapper
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ApiResponse<T> {
    
    private Boolean ok;
    private T data;
    private String error;
    private String elapsed;
    
    /**
     * Create successful response
     */
    public static <T> ApiResponse<T> success(T data) {
        return ApiResponse.<T>builder()
                .ok(true)
                .data(data)
                .elapsed("0.001s")
                .build();
    }
    
    /**
     * Create successful response with custom elapsed time
     */
    public static <T> ApiResponse<T> success(T data, String elapsed) {
        return ApiResponse.<T>builder()
                .ok(true)
                .data(data)
                .elapsed(elapsed)
                .build();
    }
    
    /**
     * Create error response
     */
    public static <T> ApiResponse<T> error(String error) {
        return ApiResponse.<T>builder()
                .ok(false)
                .error(error)
                .build();
    }
}