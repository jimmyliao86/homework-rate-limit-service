package com.example.demo.dto;

import java.util.List;

/**
 * One page of results, in a shape this project owns.
 *
 * <p>Spring Data's {@code Page} would serialise a great deal more than this -- sort
 * descriptors, {@code pageable}, {@code first}/{@code last}/{@code numberOfElements} --
 * and its JSON layout has changed between versions, which would make the API contract
 * follow a library's release notes. Five fields, declared here, do not move.
 *
 * @param content       the rules on this page, newest first
 * @param page          zero-based index of this page
 * @param size          the requested page size, not the number of rows returned; the last
 *                      page is normally shorter than this
 * @param totalElements rows in the table, not on this page
 * @param totalPages    derived from the two above rather than stored, so the three can
 *                      never disagree
 */
public record PagedResponse<T>(
        List<T> content,
        int page,
        int size,
        long totalElements,
        int totalPages) {

    /**
     * @param size must be at least 1; the controller's {@code @Min(1)} guarantees it, and
     *             a zero would otherwise divide by zero here rather than at the boundary
     *             where the caller can be told what is wrong
     */
    public static <T> PagedResponse<T> of(List<T> content, int page, int size, long totalElements) {
        // Ceiling division: 21 elements at 20 per page is 2 pages, not 1.
        int totalPages = (int) ((totalElements + size - 1) / size);
        return new PagedResponse<>(content, page, size, totalElements, totalPages);
    }
}
