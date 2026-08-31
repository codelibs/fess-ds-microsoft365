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
package org.codelibs.fess.ds.ms365.client;

/**
 * Identifies who owns a OneNote notebook, and therefore which Graph path it lives under.
 *
 * <p>Graph exposes notebooks under three different roots -- {@code /users/{id}},
 * {@code /sites/{id}} and {@code /groups/{id}} -- and the request builders for them are
 * unrelated types. Passing the owner id alone is not enough to reach the right one.</p>
 */
public enum NotebookScope {

    /** A user's personal notebooks, under {@code /users/{id}} (or {@code /me} when the id is null). */
    USER,

    /** A SharePoint site's notebooks, under {@code /sites/{id}}. */
    SITE,

    /** A Microsoft 365 group's notebooks, under {@code /groups/{id}}. */
    GROUP
}
