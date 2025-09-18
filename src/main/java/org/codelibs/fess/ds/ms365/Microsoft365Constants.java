/*
 * Copyright 2012-2024 CodeLibs Project and the Others.
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

/**
 * Constants used across Microsoft 365 data store implementations.
 *
 * @author shinsuke
 */
public final class Microsoft365Constants {

    private Microsoft365Constants() {
        // Utility class - prevent instantiation
    }

    // AdditionalData keys
    /** Key for identifying the source type of the document in additional data. */
    public static final String SOURCE_TYPE_KEY = "sourceType";
    /** Key for storing the SharePoint site ID in additional data. */
    public static final String SITE_ID_KEY = "siteId";
    /** Key for storing the SharePoint list ID in additional data. */
    public static final String LIST_ID_KEY = "listId";
    /** Key for storing the SharePoint list item ID in additional data. */
    public static final String LIST_ITEM_ID_KEY = "listItemId";
    /** Key for storing the SharePoint list title in additional data. */
    public static final String LIST_TITLE_KEY = "listTitle";
    /** Key for storing the SharePoint list item title in additional data. */
    public static final String LIST_ITEM_TITLE_KEY = "listItemTitle";
    /** Key for storing the SharePoint site name in additional data. */
    public static final String SITE_NAME_KEY = "siteName";
    /** Key for storing the SharePoint site URL in additional data. */
    public static final String SITE_URL_KEY = "siteUrl";
    /** Key for storing the SharePoint list name in additional data. */
    public static final String LIST_NAME_KEY = "listName";
    /** Key for storing the SharePoint list template type in additional data. */
    public static final String LIST_TEMPLATE_KEY = "listTemplate";
    /** Key for storing the list item creation date in additional data. */
    public static final String LIST_ITEM_CREATED_KEY = "listItemCreated";
    /** Key for storing the list item modification date in additional data. */
    public static final String LIST_ITEM_MODIFIED_KEY = "listItemModified";
    /** Key for storing the list item author in additional data. */
    public static final String LIST_ITEM_AUTHOR_KEY = "listItemAuthor";
    /** Key for storing the attachment file name in additional data. */
    public static final String ATTACHMENT_NAME_KEY = "attachmentName";

    // Source type values
    /** Source type value indicating the document is a SharePoint list attachment. */
    public static final String LIST_ATTACHMENT_SOURCE_TYPE = "ListAttachment";

    // SharePoint field mapping keys
    /** Field name for mapping SharePoint site ID in the search index. */
    public static final String SHAREPOINT_SITE_ID_FIELD = "sharepoint_site_id";
    /** Field name for mapping SharePoint list ID in the search index. */
    public static final String SHAREPOINT_LIST_ID_FIELD = "sharepoint_list_id";
    /** Field name for mapping SharePoint list item ID in the search index. */
    public static final String SHAREPOINT_LIST_ITEM_ID_FIELD = "sharepoint_list_item_id";
    /** Field name for mapping SharePoint list title in the search index. */
    public static final String SHAREPOINT_LIST_TITLE_FIELD = "sharepoint_list_title";
    /** Field name for mapping SharePoint list item title in the search index. */
    public static final String SHAREPOINT_LIST_ITEM_TITLE_FIELD = "sharepoint_list_item_title";

    // SharePoint field names
    /** SharePoint field name for the title property. */
    public static final String TITLE_FIELD = "Title";
    /** SharePoint field name for the creation date property. */
    public static final String CREATED_FIELD = "Created";
    /** SharePoint field name for the modification date property. */
    public static final String MODIFIED_FIELD = "Modified";
    /** SharePoint field name for the author property. */
    public static final String AUTHOR_FIELD = "Author";

    // Default values
    /** Default value used when the list template type is unknown. */
    public static final String UNKNOWN_TEMPLATE = "unknown";
    /** SharePoint list template type for document libraries. */
    public static final String DOCUMENT_LIBRARY = "documentLibrary";
    /** SharePoint list template type for generic lists. */
    public static final String GENERIC_LIST = "genericList";

    // SharePoint Authentication Notes
    /**
     * For SharePoint list attachments access, the Azure App Registration requires:
     *
     * 1. Microsoft Graph API Permissions (for basic site/list operations):
     *    - Sites.Read.All or Sites.ReadWrite.All
     *    - Files.Read.All (if accessing files)
     *
     * 2. SharePoint Permissions (for REST API access to attachments):
     *    - The app must be granted access to the specific SharePoint tenant
     *    - Scope: https://{tenant}.sharepoint.com/.default
     *    - Note: Some endpoints may require delegated permissions rather than app-only
     *
     * 3. Authentication Flow:
     *    - Use Microsoft Graph SDK for site/list metadata
     *    - Use SharePoint REST API with proper scopes for attachments
     *    - Token scope must match the target API (Graph vs SharePoint)
     */
}