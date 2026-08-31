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
 * Thrown when a document's permissions could not be retrieved.
 *
 * <p>Extends {@link CrawlingAccessException} so that each data store's existing
 * per-item handler records it against the failure URL list and moves on, instead of
 * indexing the document with an empty ACL. An empty ACL is not a neutral outcome:
 * it removes the document from every user's results, or -- when default_permissions
 * is set -- publishes it more widely than intended.</p>
 */
public class PermissionUnavailableException extends CrawlingAccessException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates an exception for a target whose permissions could not be read.
     *
     * @param message the detail message
     * @param cause the failure that prevented the lookup
     */
    public PermissionUnavailableException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
