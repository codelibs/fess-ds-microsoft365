/*
 * Copyright 2012-2025 CodeLibs Project and the Others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.codelibs.fess.ds.ms365;

import org.codelibs.fess.crawler.exception.CrawlingAccessException;

/**
 * Thrown when a file's sensitivity labels could not be read, or could not be matched against the
 * configured label policy.
 *
 * <p>Extends {@link CrawlingAccessException} for the same reason as
 * {@link PermissionUnavailableException}: the per-item handler records it against the failure URL
 * list and moves on. Indexing the file as if it were unlabeled would bypass every rule the
 * operator configured for its label.</p>
 */
public class SensitivityLabelUnavailableException extends CrawlingAccessException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates an exception for a file whose labels could not be read.
     *
     * @param message the detail message
     * @param cause the failure that prevented the lookup, or {@code null}
     */
    public SensitivityLabelUnavailableException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
