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

import java.util.Map;

/**
 * Constants used across Microsoft 365 data store implementations.
 *
 * @author shinsuke
 */
public final class Microsoft365Constants {

    private Microsoft365Constants() {
        // Utility class - prevent instantiation
    }

    // Default values
    /** Default value used when the list template type is unknown. */
    public static final String UNKNOWN_TEMPLATE = "unknown";
    /** SharePoint list template type for document libraries. */
    public static final String DOCUMENT_LIBRARY = "documentLibrary";
    /** SharePoint list template type for generic lists. */
    public static final String GENERIC_LIST = "genericList";

    /** SharePoint list template type IDs mapped to the template names Graph reports. */
    private static final Map<String, String> TEMPLATE_NAMES_BY_ID = Map.of("100", GENERIC_LIST, "101", DOCUMENT_LIBRARY, "102", "survey",
            "103", "links", "104", "announcements", "105", "contacts");

    /**
     * Translates a SharePoint list template ID to the template name Graph reports.
     *
     * <p>Only the IDs Microsoft documents against Graph's {@code list.template} are mapped.
     * Higher IDs exist in SharePoint's own SPListTemplateType enumeration, but Graph's
     * documented names for them are not published, and guessing at them would recreate the
     * silent no-match this mapping exists to fix.</p>
     *
     * @param id the numeric template ID
     * @return the Graph template name, or null if the ID has no documented mapping
     */
    public static String templateNameForId(final String id) {
        return TEMPLATE_NAMES_BY_ID.get(id);
    }
}