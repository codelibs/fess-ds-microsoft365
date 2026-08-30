# Microsoft365 Data Store for Fess

[![Java CI with Maven](https://github.com/codelibs/fess-ds-microsoft365/actions/workflows/maven.yml/badge.svg)](https://github.com/codelibs/fess-ds-microsoft365/actions/workflows/maven.yml)
[![Maven Central](https://img.shields.io/maven-central/v/org.codelibs.fess/fess-ds-microsoft365.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22org.codelibs.fess%22%20AND%20a:%22fess-ds-microsoft365%22)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

A powerful Fess Data Store plugin that enables enterprise search across Microsoft 365 services including OneDrive, OneNote, Teams, SharePoint Document Libraries, SharePoint Lists, and SharePoint Pages via Microsoft Graph API v6.

## Overview

This plugin extends [Fess](https://fess.codelibs.org/) enterprise search capabilities to comprehensively index Microsoft 365 content, providing unified search across your organization's cloud documents, conversations, and data with role-based access control integration.

## ✨ Key Features

### 📁 **Comprehensive Content Crawling**
- **OneDrive**: User drives, group drives, shared documents, and specific drives with metadata extraction
- **OneNote**: Complete notebooks with aggregated content from all sections and pages, supporting site, user, and group notebooks
- **Teams**: Channels, messages, chats with conversation context
- **SharePoint Document Libraries**: Document library metadata indexing (libraries crawled as searchable entities, not individual files)
- **SharePoint Lists**: Custom lists and list items with dynamic field mapping
- **SharePoint Pages**: Site pages and news articles with full content extraction

### 🔐 **Enterprise-Grade Security**
- **Role-based Access Control**: Seamless integration with Fess security model
- **Azure AD Authentication**: Client credentials flow with automatic token refresh
- **Permission Inheritance**: Preserves Microsoft 365 access permissions in search results

### ⚡ **Performance & Reliability**
- **Microsoft Graph SDK v6**: Latest API with efficient pagination and caching
- **Multi-threaded Processing**: Configurable thread pools for optimal performance
- **Smart Caching**: User type, group ID, UPN and group name caching to reduce API calls, bounded by `cache_size`
- **Robust Error Handling**: Comprehensive error tracking with configurable failure recovery
- **Content Filtering**: Advanced include/exclude patterns with system content filtering

### 🛠 **Developer-Friendly**
- **Maven Integration**: Clean build process with dependency shading
- **Extensive Testing**: UTFlute-based test framework with mock Graph API responses
- **Configurable Field Mapping**: Customizable data extraction scripts for each service

## 🚀 Quick Start

### Prerequisites

- **Java**: 21 or higher
- **Fess**: 15.2.0 or higher
- **Azure AD**: App registration with Microsoft Graph API permissions

### Installation

#### Option 1: Download Pre-built JAR
1. Download the latest `fess-ds-microsoft365-X.X.X.jar` from [Maven Central](https://repo1.maven.org/maven2/org/codelibs/fess/fess-ds-microsoft365/)
2. Copy the JAR file to your Fess installation:
   ```bash
   # For standard installation
   cp fess-ds-microsoft365-X.X.X.jar $FESS_HOME/app/WEB-INF/lib/
   
   # For system-wide installation
   sudo cp fess-ds-microsoft365-X.X.X.jar /usr/share/fess/app/WEB-INF/lib/
   ```
3. Restart Fess service

#### Option 2: Build from Source
```bash
# Clone the repository
git clone https://github.com/codelibs/fess-ds-microsoft365.git
cd fess-ds-microsoft365

# Build the plugin
mvn clean package

# Copy to Fess installation
cp target/fess-ds-microsoft365-*.jar $FESS_HOME/app/WEB-INF/lib/
```

### Azure App Registration Setup

Before using this plugin, create an Azure App registration with the required permissions:

1. **Register Application** in Azure Portal
2. **Add API Permissions** (Microsoft Graph) - see [Required Permissions by DataStore](#required-permissions-by-datastore) below
3. **Grant Admin Consent** for the permissions
4. **Create Client Secret** and note the values

### Required Permissions by DataStore

Each DataStore requires specific Microsoft Graph API permissions. Grant only the permissions needed for your use case.

| DataStore | Required Permissions | Conditional Permissions |
|-----------|---------------------|------------------------|
| OneDriveDataStore | Files.Read.All, User.Read.All, Group.Read.All | Sites.Read.All (*1) |
| OneNoteDataStore | Notes.Read.All | User.Read.All (*2), Group.Read.All (*3), Sites.Read.All (*4) |
| TeamsDataStore | Team.ReadBasic.All, Channel.ReadBasic.All, ChannelMessage.Read.All, ChannelMember.Read.All, Group.Read.All, User.Read.All | Chat.Read.All (*5), attachment permission (*6) |
| SharePointDocLibDataStore | Sites.Read.All, User.Read.All, Group.Read.All | - |
| SharePointListDataStore | Sites.Read.All (*7) | - |
| SharePointPageDataStore | Sites.Read.All (*7) | - |

`User.Read.All` and `Group.Read.All` are unconditional for OneDriveDataStore and
SharePointDocLibDataStore: every drive-item ACL entry resolves its grantee's UPN or group name via
`GET /users/{id}` / `GET /groups/{id}` (`processDriveItem` builds this ACL on every crawling mode,
not just user/group drives), regardless of which crawling mode found the item. `Files.Read.All` is
not listed for SharePointDocLibDataStore: the two endpoints it needs beyond site enumeration -
`GET /sites/{id}/drives` and `GET /drives/{id}/items/root/permissions` - both accept `Sites.Read.All`,
which the DataStore already needs to enumerate sites, so a separate `Files.Read.All` grant would buy
nothing.

**Conditional Permission Notes:**
- (*1) Required when `shared_documents_drive_crawler=true` (default: true). In that mode
  OneDriveDataStore enumerates every SharePoint site (`GET /sites`) and every subsite (`GET
  /sites/{site-id}/sites`, see the subsite note below) to find their document libraries; neither
  call accepts `Files.Read.All` as an alternative.
- (*2) Required when `user_note_crawler=true` (default: true)
- (*3) Required when `group_note_crawler=true` (default: true)
- (*4) Required when `site_note_crawler=true` (default: true), to resolve the root site
  (`GET /sites/root`) that site notebooks are enumerated under
- (*5) Required when `chat_id` is specified
- (*6) TeamsDataStore fetches attachment content via `GET /shares/{id}/driveItem/content`, whose
  Microsoft-documented Application permissions are `Files.ReadWrite.All` (least privileged) or
  `Sites.ReadWrite.All` (higher) - a write-class grant, even though this plugin only reads the
  attachment. `Files.Read.All` is not listed as acceptable for this call at all. `append_attachment`
  defaults to `true`, so this permission is required unless you set `append_attachment=false`,
  which skips attachment content entirely and keeps the grant read-only.
- (*7) Can be replaced with `Sites.Selected` when `site_id` is specified - see
  [Using Sites.Selected Permission](#using-sitesselected-permission) below.

**Subsites:** `GET /sites/{site-id}/sites` recursion is used by OneDriveDataStore (in
shared-documents mode), SharePointDocLibDataStore, SharePointListDataStore, and
SharePointPageDataStore to discover every child site under a parent site. Each subsite it returns
is treated as an ordinary site - itself enumerated for content, and itself capable of having
further subsites - under the same `Sites.Read.All` (or `Sites.Selected`) grant.

#### Using Sites.Selected Permission

When `site_id` is specified, `Sites.Selected` is a genuine least-privilege alternative to
`Sites.Read.All` for SharePointDocLibDataStore, SharePointListDataStore and SharePointPageDataStore:

1. Grant `Sites.Selected` permission to your app registration in Azure Portal
2. Use Microsoft Graph PowerShell or API to grant access to specific sites
3. Explicitly allow access for each target site

`Sites.Selected` cannot serve `GET /sites` (listing every site in the tenant), which is why it only
helps when `site_id` restricts the crawl to sites you have explicitly granted the app access to;
without `site_id`, these three DataStores still need `Sites.Read.All`.

Reference: https://learn.microsoft.com/en-us/graph/permissions-reference#sitesselected

> **No individual Microsoft Graph API reference page lists `Sites.Selected`** in its per-endpoint
> permissions table - not `site-get`, not `list-list`, not `driveitem-list-permissions`, none of the
> endpoints these three DataStores call. The `Sites.Selected` model is documented only in the
> [permissions reference](https://learn.microsoft.com/en-us/graph/permissions-reference#sitesselected)
> and the site-level access-grant APIs, not as a row in any individual endpoint's table. Treat it as
> Microsoft's documented site-access model rather than a per-endpoint guarantee.

#### What ACL Each DataStore Can Produce

An administrator needs to predict what roles a crawled document will carry, not just what
permissions it took to crawl it:

| DataStore | Roles on each document |
|-----------|------------------------|
| OneDriveDataStore | `GET /drives/.../permissions` (per-user/per-group grantees and organization-scope sharing links) plus `default_permissions` |
| SharePointDocLibDataStore | `GET /drives/.../permissions` (per-user/per-group grantees and organization-scope sharing links) plus `default_permissions` |
| SharePointListDataStore | `default_permissions` only |
| SharePointPageDataStore | `default_permissions` only |
| OneNoteDataStore | User/group notebooks: a role synthesized from the owner's id, plus `default_permissions`. Site notebooks: `default_permissions` only |
| TeamsDataStore | Channel or chat membership plus `default_permissions` |

**Every row above also carries the data config's own Permissions field**, on top of what the row
names. That is the field on the Fess data config itself, which is a separate setting from the
`default_permissions` crawl parameter; both end up in the same role list, de-duplicated. This is
worth stating for all six rows because it was not true of all six until this release:
TeamsDataStore silently discarded it, and now applies it - see
[Re-crawling after upgrading to the Teams fixes](#re-crawling-after-upgrading-to-the-teams-fixes)
below before re-crawling a Teams config.

Microsoft Graph exposes no app-only way to read SharePoint's own user and group role assignments
for a site, list, list item or page. Microsoft's own Azure AI Search SharePoint ACL indexer
requires `Sites.FullControl.All` or a full-control-class `Sites.Selected` grant to do the
equivalent lookup, in every documented scenario. This plugin does not attempt it: for
SharePointListDataStore, SharePointPageDataStore, and OneNoteDataStore's site notebooks,
`default_permissions` is the only way those documents get an audience - leave it unset and they
are indexed with no role at all, which means they are findable by **nobody**, not by everybody.

**Upgrading from 15.8.0:** SharePointListDataStore and SharePointPageDataStore documents were
already being indexed, and already carried a `default_permissions`-plus-config-Permissions ACL,
before this release. The removed site-permissions lookup caught every exception it could raise -
including the `403` a tenant without `Sites.FullControl.All` got - logged it at `WARN`, and
returned an empty list rather than failing the item; `default_permissions` and the data config's
Permissions field were appended to that empty list either way, and the item or page was indexed.
So their audience does not change on upgrade. What does change: `Sites.FullControl.All` is no
longer needed at all, and the per-site `Failed to retrieve permissions for site` `WARN` these
tenants have been seeing on every crawl is gone, because the call that produced it no longer
exists.

The one genuinely new thing in the index after upgrading is OneNote site notebooks. They were
missing under 15.8.0 because the notebook, section, page, and content requests for them were sent
to the wrong Graph path and 404'd - not because of a permissions problem. See [Re-crawling after
upgrading to this fix](#re-crawling-after-upgrading-to-this-fix) below for how that ACL is built.

As always: if `default_permissions` is left unset on SharePointListDataStore,
SharePointPageDataStore, or OneNoteDataStore's site notebooks, those documents carry no role at
all and are searchable by nobody, and nothing in the logs flags it - only this note does.

**Organization-shared OneDrive files:** a document whose only Graph permission is an
organization-scope sharing link (`link.scope == "organization"`, i.e. "Anyone in your organization
with the link") previously carried no roles at all on the OneDrive path, so it was findable by
nobody. It now carries a single role, the group named `EVERYONE_IN_TENANT`, which also matches
nobody unless the operator creates an actual group named `EVERYONE_IN_TENANT` and maps it to a
Fess role. This is not a loss of visibility (those files were already unfindable), but it is the
only way organization-shared OneDrive files become searchable, and the sentinel group name appears
nowhere else in this document.

**Precedence between a named grantee and a link's scope:** when one permission carries both a
named grantee (`grantedToV2`, or an entry in `grantedToIdentitiesV2`) and a `link`, only the
grantee's role is added; the permission does not also widen the ACL to the link's scope. This is
deliberate and fail-closed - a permission that already names who it is for should not also be
read as "anyone in the organization" - but it narrows one ACL shape SharePointDocLibDataStore
already produced before this branch. Its root-drive permission lookup has always run through this
same code, and the previous version of that code ignored `grantedToIdentitiesV2` entirely, so a root
permission shaped like `{link: {scope: "organization"}, grantedToIdentitiesV2: [user A]}` used to
contribute `EVERYONE_IN_TENANT` in addition to A's role. Now it contributes only A's role.
SharePointDocLibDataStore indexes one document per document library (library metadata, not its
individual files - see the Data Store Types table below), and computes that document's entire ACL
once, from the drive's root permissions. So after upgrading, a document library whose root
permission names grantees alongside an organization-scope link loses `EVERYONE_IN_TENANT` from
that library's indexed document.

#### Minimum Permissions Examples

**OneDrive only (user drives):**
```
Files.Read.All, User.Read.All, Group.Read.All
```

**SharePoint Lists (specific site):**
```
Sites.Selected (with per-site configuration)
```

**Teams (channels only, no chat, read-only):**
```
Team.ReadBasic.All, Channel.ReadBasic.All, ChannelMessage.Read.All, ChannelMember.Read.All, Group.Read.All, User.Read.All
```
Requires `append_attachment=false` (default is `true`) to stay within this permission set - see
conditional note (*6) above.

### Basic Configuration

Configure the following authentication parameters in Fess:

```properties
# Required Azure AD credentials
tenant=********-****-****-****-************
client_id=********-****-****-****-************  
client_secret=***********************

# Optional performance settings
number_of_threads=1
ignore_error=false
```

## 📊 Data Store Types

The plugin provides six specialized data store types, each optimized for different Microsoft 365 services:

| Data Store | Service | Content Types | Use Cases |
|------------|---------|---------------|----------|
| `oneDriveDataStore` | OneDrive | Files, Folders, Metadata | Document search, file discovery |
| `oneNoteDataStore` | OneNote | Notebooks (with sections & pages content) | Knowledge base search, note finding, documentation search |
| `teamsDataStore` | Teams | Channels, Messages, Chats | Conversation search, team communication |
| `sharePointDocLibDataStore` | SharePoint | Document Libraries (metadata only) | Document library discovery and metadata search |
| `sharePointListDataStore` | SharePoint | Lists, List Items, Custom Fields | Structured data search, business process content |
| `sharePointPageDataStore` | SharePoint | Site Pages, News Articles, Wiki Pages | Web content search, intranet portal search |

### Configuration in Fess Admin UI

1. Navigate to **System > Data Store**
2. Click **Create New**
3. Select the desired data store type from the dropdown
4. Configure authentication and crawling parameters
5. Save and start crawling

### Scripts

#### OneDrive

```
title=file.name
content=file.description + "\n" + file.contents
mimetype=file.mimetype
created=file.created
last_modified=file.last_modified
url=file.web_url
role=file.roles
```

| Key | Value |
| --- | --- |
| file.name | The name of the file. |
| file.description | A short description of the file. |
| file.contents | The text contents of the file (extracted using Tika) |
| file.mimetype | The MIME type of the file. |
| file.filetype | The file type category determined by MIME type. |
| file.created | The time at which the file was created. |
| file.last_modified | The last time the file was modified by anyone. |
| file.size | The size of the file in bytes. |
| file.web_url | A link for opening the file in an editor or viewer in a browser. |
| file.url | The processed URL for the file (may differ from web_url for certain locations). |
| file.roles | Users/groups who can access the file. |
| file.ctag | Change tag for the file (used for change tracking). |
| file.etag | Entity tag for the file (used for caching). |
| file.id | The unique identifier of the file in OneDrive. |
| file.webdav_url | WebDAV URL for the file (if available). |
| file.location | Geographic location metadata (if available). |
| file.createdby_application | Application that created the file. |
| file.createdby_device | Device that created the file. |
| file.createdby_user | User who created the file. |
| file.deleted | Deletion information (if file was deleted). |
| file.hashes | File hash values for integrity checking. |
| file.last_modifiedby_application | Application that last modified the file. |
| file.last_modifiedby_device | Device that last modified the file. |
| file.last_modifiedby_user | User who last modified the file. |
| file.image | Image metadata (for image files). |
| file.parent | Parent reference information. |
| file.parent_id | ID of the parent folder. |
| file.parent_name | Name of the parent folder. |
| file.parent_path | Path to the parent folder. |
| file.photo | Photo metadata (for photo files). |
| file.publication | Publication information (if applicable). |
| file.search_result | Search result metadata (if file was found via search). |
| file.special_folder | Special folder name (if file is in a special folder). |
| file.video | Video metadata (for video files). |

#### OneNote

```
title=notebook.name
content=notebook.contents
created=notebook.created
last_modified=notebook.last_modified
url=notebook.web_url
role=notebook.roles
size=notebook.size
```

| Key | Value |
| --- | --- |
| notebook.name | The name of the notebook. |
| notebook.contents | The extracted text contents from all sections and pages within the notebook. |
| notebook.size | The size of the notebook content in characters. |
| notebook.created | The time at which the notebook was created. |
| notebook.last_modified | The last time the notebook was modified by anyone. |
| notebook.web_url | A link for opening the notebook in OneNote web or desktop app. |
| notebook.roles | Users/groups who can access the notebook. |

#### Teams

```
title=message.title
content=message.content
created=message.created_date_time
last_modified=message.last_modified_date_time
url=message.web_url
role=message.roles
```

| Key | Value |
| --- | --- |
| message.title | The message title (sender name and timestamp). |
| message.content | The text contents of the message including attachments if configured. |
| message.created_date_time | The time at which the message was created. |
| message.last_modified_date_time | The last time the message was modified. |
| message.web_url | A link for opening the message in Teams. |
| message.roles | Users/groups who can access the team/channel/chat. |
| message.id | The unique identifier of the message. |
| message.from | The sender information. |
| message.subject | The subject of the message. |
| message.body | The body content with type information. |
| message.attachments | File attachments associated with the message. |
| message.mentions | Users mentioned in the message. |
| team | The team object containing team information (when applicable). |
| channel | The channel object containing channel information (when applicable). |
| parent | The parent message for replies (when applicable). |

#### SharePoint Document Libraries

```
title=doclib.name
content=doclib.content
created=doclib.created
last_modified=doclib.modified
url=doclib.url
role=doclib.roles
```

| Key | Value |
| --- | --- |
| doclib.id | The unique identifier of the document library (Drive ID). |
| doclib.name | The name of the document library. |
| doclib.description | The description of the document library. |
| doclib.content | Rich content combining document library name, description, and site name for enhanced search. |
| doclib.web_url | The Microsoft Graph API web URL for the document library. |
| doclib.url | The standardized SharePoint URL for accessing the document library. |
| doclib.created | The time at which the document library was created. |
| doclib.modified | The last time the document library was modified. |
| doclib.type | The type of the drive (e.g., "documentLibrary"). |
| doclib.site_name | The display name of the SharePoint site containing this document library. |
| doclib.site_url | The web URL of the SharePoint site. |
| doclib.roles | Users/groups who can access the document library. |

**Important**: SharePointDocLibDataStore indexes document libraries themselves as searchable entities (not the files within them). Each document library becomes one search result containing aggregated metadata including library name, description, and parent site information. For individual file indexing within SharePoint document libraries, use the OneDriveDataStore which handles SharePoint document library files through the Microsoft Graph Drive API.

#### SharePoint Lists

```
title=item.title
content=item.content
created=item.created
last_modified=item.modified
url=item.url
role=item.roles
```

| Key | Value |
| --- | --- |
| item.title | The title of the list item (extracted from Title, LinkTitle, or FileLeafRef fields). |
| item.content | The text contents of the list item (extracted from Body, Description, Comments, or Notes fields) |
| item.id | The unique identifier of the list item |
| item.created | The time at which the list item was created. |
| item.modified | The last time the list item was modified. |
| item.url | A link for opening the list item in SharePoint. |
| item.fields | All fields and values from the SharePoint list item as a map |
| item.roles | Users/groups who can access the list item. |
| item.site | Site information containing `id`, `name`, and `url` |
| item.list | List information containing `name`, `description`, `url`, and `template_type` |

**Data Structure**: The `item` object contains nested structures:
- `item.site` - Contains site metadata (site.id, site.name, site.url)
- `item.list` - Contains list metadata (list.name, list.description, list.url, list.template_type)
- `item.fields` - Dynamic map of all SharePoint list fields and their values

**Note**: The plugin automatically expands SharePoint list item fields to ensure content extraction. If fields are not initially available, it performs an individual API call with `$expand=fields` to retrieve the complete field data.

#### SharePoint Pages

```
title=page.title
content=page.content
created=page.created
last_modified=page.modified
url=page.url
role=page.roles
```

| Key | Value |
| --- | --- |
| page.title | The title of the SharePoint page. |
| page.content | The extracted text content from the page canvas layout including web parts. |
| page.id | The unique identifier of the page. |
| page.created | The time at which the page was created. |
| page.modified | The last time the page was modified. |
| page.author | The user who created the page. |
| page.type | The type of page (news, article, page). |
| page.description | The page description or summary. |
| page.url | A link for opening the page in SharePoint. |
| page.canonical_url | The canonical URL for accessing the page. |
| page.promotion_state | The promotion status of the page (for news pages). |
| page.site_name | The display name of the SharePoint site containing this page. |
| page.site_url | The web URL of the SharePoint site. |
| page.roles | Users/groups who can access the page. |

**Content Extraction**: The SharePointPageDataStore extracts content from:
- **Page Title**: The main page title
- **Page Description**: Page description or summary text
- **Canvas Layout**: Text content from web parts (TextWebPart, StandardWebPart)
- **Web Parts**: HTML content converted to plain text with proper formatting

**Page Types**: The plugin automatically detects and categorizes pages:
- `news`: News posts and announcements
- `article`: Article pages and documentation
- `page`: Standard site pages

**Note**: Content extraction from canvas layout depends on the Microsoft Graph SDK's ability to retrieve web part data. The plugin handles both text web parts and attempts to extract meaningful content from standard web parts when possible.

## ⚙️ Configuration Reference

### Authentication Parameters (Required)

| Parameter | Description | Example |
|-----------|-------------|----------|
| `tenant` | Azure AD tenant ID | `contoso.onmicrosoft.com` or GUID |
| `client_id` | App registration client ID | `12345678-1234-1234-1234-123456789abc` |
| `client_secret` | App registration client secret | `abcdefghijk...` |

### Common Crawling Parameters

| Parameter | Description | Default | Example |
|-----------|-------------|---------|----------|
| `number_of_threads` | Concurrent crawling threads | `1` | `3` |
| `executor_shutdown_timeout` | How long to wait for submitted crawling tasks to finish, in whole seconds. Whatever has not finished by then is cancelled and its documents are missing from the crawl, which is reported at `ERROR` - see below. Raise it for a large tenant. A value that is not a positive whole number keeps this default and logs a `WARN`. | `60` | `600` |
| `ignore_error` | Continue on errors. What "errors" means differs by DataStore - see below | `false` | `true` |
| `include_pattern` | Regex pattern for inclusion - semantics differ by DataStore, see below | - | `.*\.pdf$` |
| `exclude_pattern` | Regex pattern for exclusion - semantics differ by DataStore, see below | - | `.*temp.*` |
| `default_permissions` | Default role assignments | - | `{role}admin` |
| `permission_failure_policy` | What to do when a document's permissions cannot be retrieved | `skip` | `index_without_acl` |
| `connect_timeout` | Connect timeout for Microsoft Graph HTTP requests, in whole seconds - see below | `100` | `30` |
| `read_timeout` | Read timeout for Microsoft Graph HTTP requests, in whole seconds - see below | `100` | `30` |
| `access_timeout` | Overall timeout for a Microsoft Graph HTTP call, in whole seconds - see below | `100` | `120` |
| `max_retry_count` | Maximum automatic retries for a failed Graph request - see below | `3` | `5` |
| `retry_interval` | Delay between automatic retries, in whole seconds - see below | `3` | `10` |
| `cache_size` | Maximum number of entries in each of the client's four lookup caches (user type, group IDs by mail, user object ID to UPN, group object ID to name) - see below | `10000` | `50000` |
| `max_content_length` | Content length cap in bytes, applied at two different points - see [`max_content_length` is applied twice](#max_content_length-is-applied-twice) below | `-1` (defer to Fess's own per-MIME-type limit, **not** unlimited) | `10485760` |
| `proxy_host` | HTTP proxy host for both Azure AD token acquisition and Microsoft Graph calls - see below | - | `proxy.example.com` |
| `proxy_port` | HTTP proxy port. Required alongside `proxy_host`; neither takes effect on its own - see below | - | `8080` |
| `proxy_username` | Proxy user name. Only used when `proxy_password` is also set - see below | - | `crawler` |
| `proxy_password` | Proxy password. Only used when `proxy_username` is also set - see below | - | `********` |
| `additionally_allowed_tenants` | Tenant IDs the credential may also acquire tokens for, comma-separated, or `*` for any tenant - see below | - (only the configured `tenant`) | `*` |

#### `include_pattern` / `exclude_pattern` semantics differ by DataStore

`include_pattern` and `exclude_pattern` are accepted by several DataStores in this plugin, but
each one matches them against different content, using different regex semantics, and none of
that is visible from the table above:

| DataStore | Matched against | Match mechanism |
|-----------|------------------|------------------|
| `oneDriveDataStore` | the generated drive-item URL (indexed as `file.url`) - **not** the raw Graph `webUrl` (`file.web_url`); the two usually agree, but diverge for `/_layouts/` paths, which `getUrl()` rewrites | Fess `UrlFilter` - full match (`Matcher.matches()`) |
| `sharePointDocLibDataStore` | the document library's **canonical URL** (`doclib.url`, built by `generateDocumentLibraryUrl(site, drive)`) - **not** the raw Graph `webUrl` (`doclib.web_url`) or the library's display name | Fess `UrlFilter` - full match (`Matcher.matches()`) |
| `sharePointListDataStore` | the list item's title (`Title`/`LinkTitle`/`FileLeafRef`, whichever resolves first) | `java.util.regex.Pattern.matches()` - full match |
| `sharePointPageDataStore` | the page's `webUrl` | `java.util.regex.Pattern.find()` - **partial** match |
| `oneNoteDataStore` | the notebook's display name | `java.util.regex.Pattern.matches()` - full match |
| `teamsDataStore` | not supported - both parameters are silently ignored | - |

Practically: a OneDrive or SharePoint document library pattern must match the *entire* URL, a
SharePoint List pattern must match the *entire* title, and a SharePoint Pages pattern only needs
to match *somewhere inside* the URL. A pattern written for one DataStore will not necessarily
behave the same way on another.

A OneNote pattern must also match the *entire* notebook display name: `exclude_pattern=Test.*`
removes a notebook called "Test Notebook" but keeps one called "Production Test Notes" - the name
contains "Test", but does not *start* with it, so the full match fails and the notebook is kept.
(A partial-match DataStore such as `sharePointPageDataStore` would drop it instead.)

The filter is applied once per notebook, in the `getNotebooks` callback for each of the SITE,
USER and GROUP scopes, before that notebook is handed off for processing. It therefore selects
whole notebooks only: there is no way to admit a notebook but exclude one of its sections or
pages, because `oneNoteDataStore` indexes one document per notebook and never inspects section or
page names to filter on. A notebook whose display name is missing, empty **or whitespace-only** is
matched as the empty string, like any other name - the three are treated as one thing, so a pattern
such as `.+` cannot admit one and reject another: with `include_pattern` set such a notebook is
excluded unless that pattern matches `""`, and with only `exclude_pattern` set it is kept unless
that pattern matches `""`. A name with any other character is matched verbatim, surrounding
whitespace included.

Note that `oneNoteDataStore` ignored both parameters in earlier releases - a configuration that
set them expecting them to be a no-op will start filtering notebooks after upgrading.

#### `ignore_error` scope differs by DataStore

`ignore_error` defaults to **`false`** in all six DataStores. It is not a blanket "keep going"
switch: each DataStore relaxes a specific, fixed set of failures, and the rest behave the same way
at either setting.

| DataStore | What `ignore_error=true` tolerates |
|-----------|------------------------------------|
| `oneDriveDataStore` | a failure extracting one file's content (the document is still indexed, without contents) |
| `sharePointDocLibDataStore` | a site whose drives cannot be listed, and a document library that fails to process |
| `sharePointListDataStore` | a site that fails to process, a list that fails to process, and a list item that fails to process |
| `sharePointPageDataStore` | a site that fails to process and a page that fails to process |
| `oneNoteDataStore` | nothing - it does not read this parameter; a user or group whose notebooks cannot be listed is logged at `WARN` and skipped either way |
| `teamsDataStore` | an unresolvable `team_id`, an unresolvable `channel_id`, a failure listing an explicitly configured team's channels, and a failure fetching a channel's messages |

`ignore_error` never widens a crawl. A `team_id` listed in `exclude_team_ids` that cannot be
resolved still aborts the crawl even with `ignore_error=true`, so that a team you asked to exclude
is never silently crawled.

#### `cache_size` and the client's lookup caches

`cache_size` bounds each of the four Guava caches the Graph client keeps, **individually** - it is
not a total across them. They are: user-type (is this object ID a user or a group?), group IDs by
mail address, user object ID to UPN, and group object ID to group name. The last two back the ACL
roles OneDriveDataStore and SharePointDocLibDataStore add alongside each raw object ID.

The default is `10000` entries per cache. A negative or non-numeric value logs a `WARN` and falls
back to that default; `0` is accepted as written and disables caching, so every lookup goes back to
Microsoft Graph. The caches are an optimisation, never a correctness requirement - a bad value
must not stop the client from being constructed - and they are dropped when the crawl closes the
client.

#### `max_content_length` is applied twice

The same parameter name is read by two different pieces of code, at two different points in a
crawl, with two different consequences. Both are **byte** counts, and neither truncates: content
over the cap is rejected, not shortened.

- **A pre-download size check, in OneDriveDataStore only.** Before a file is fetched, the drive
  item's Graph-reported `size` is compared against the cap. Over it, the item is **not indexed at
  all**: a `MaxLengthExceededException` is raised and the item is recorded in the failed-URL list.
  This is the only place `max_content_length` decides whether a document exists.
- **An extractor input cap, wherever text is extracted.** The value is handed to Fess's extractor
  as `maxContentLength(...)`, which measures the bytes it actually read and throws
  `MaxLengthExceededException` if they exceed it. Three call sites use it: OneDrive file contents,
  OneNote page contents, and **Teams attachment text** (reached whenever `append_attachment=true`,
  which is the default). Failing here does not necessarily drop the document - Fess's
  `crawler.ignore.content.exception` decides, and for OneDrive files `ignore_error` does too; when
  it is tolerated the document is indexed with empty contents for that source.

`-1` (the default) does **not** mean unlimited at either point. It means "defer to Fess", and the
effective cap becomes `ContentLengthHelper`'s limit for that MIME type, or its default limit when
the MIME type is unknown - which it always is for OneNote page content, since that extraction
passes neither a MIME type nor a filename.

Two further asymmetries are worth knowing. OneDriveDataStore parses the value as a `long`, while
the Graph client parses it as an `int`: a value above `2147483647` is accepted by the first and
logged as unparseable by the second, which then keeps `-1`. And a Teams attachment or a OneNote
page is never skipped in advance on size - only its extraction can fail - because neither has a
Graph-reported size the way a drive item does.

#### Proxy parameters

`proxy_host` and `proxy_port` take effect only when **both** are set; either one alone is ignored
with no warning. When both are set, the proxy is applied to two independent HTTP stacks: the Azure
Identity client that acquires the OAuth token, and the OkHttp client the Microsoft Graph SDK uses
for every API call. A `proxy_port` that is not a number aborts the crawl with
`DataStoreException: Invalid proxy port`.

`proxy_username` and `proxy_password` are likewise applied only when **both** are non-empty; a user
name with no password (or the reverse) is silently ignored and the proxy is used unauthenticated.
Graph requests authenticate to the proxy with HTTP Basic via a `Proxy-Authorization` header.

See [`additionally_allowed_tenants` and the Graph host allowlist](#additionally_allowed_tenants-and-the-graph-host-allowlist)
below for a bearer-token leak that used to affect proxied deployments specifically.

#### Permission lookup failures

This applies to OneDriveDataStore and SharePointDocLibDataStore, where a document's ACL comes from
a separate Microsoft Graph call (`GET /drives/{id}/items/{id}/permissions`, listing its
permissions) that can fail on its own even when the document itself was read successfully -
typically a transient `429` or `503` under throttling.

SharePointListDataStore, SharePointPageDataStore, and OneNoteDataStore do not call this or any
other permission-fetch endpoint, so `permission_failure_policy` has no effect on any of them:
SharePointListDataStore and SharePointPageDataStore rely solely on `default_permissions` (see
[What ACL Each DataStore Can Produce](#what-acl-each-datastore-can-produce) above); OneNoteDataStore's
user and group notebooks resolve permissions from data already in hand while enumerating - the
same mechanism OneDriveDataStore uses for its own personal and group drives, synthesizing a role
from the owner's ID rather than calling Graph for it - and its site notebooks rely solely on
`default_permissions` too. TeamsDataStore does not call any of the permission-fetch methods this
parameter governs either, so this parameter has no effect for it too - but not because
TeamsDataStore has no permission-fetch failure path of its own. It makes a separate Graph call per
message (listing the channel's members, to resolve that message's roles), and a failure there is
not governed by `permission_failure_policy` at all: it propagates out of the per-channel handler as
a `DataStoreException`, which aborts the rest of that channel's messages rather than skipping just
the one message.

`permission_failure_policy` controls what happens when the permissions call fails for the
DataStores and paths it covers - OneDriveDataStore and SharePointDocLibDataStore - and takes one of
two values:

| Value | Behavior |
|-------|----------|
| `skip` (default) | The document is not indexed. The failure is recorded in the failed URL list and logged at `WARN`, so it is visible after the crawl. |
| `index_without_acl` | The document is indexed with whatever permissions were collected before the failure, possibly none. This was the only behavior before this parameter existed. |

An unrecognized value is logged at `WARN` and treated as `skip`. There is no value that aborts
the crawl: a permission lookup always runs inside a per-item handler that already catches every
exception for that item and moves on to the next one, so nothing thrown while resolving
permissions can stop the crawl.

Indexing a document with an incomplete ACL is not a neutral fallback. An empty ACL removes the
document from every user's search results until it is re-crawled successfully, and when
`default_permissions` is configured, `index_without_acl` instead publishes the document to
everyone that setting covers - more widely than intended, because the per-user/per-group roles
that should have narrowed access down were never added. Choose `index_without_acl` only if
surfacing a document without its full ACL is preferable to not surfacing it at all.

`ignore_error` and `permission_failure_policy` cover different failures. `ignore_error` governs
failures while enumerating and processing containers and their contents - sites, drives, lists,
list items, pages, and (since Teams started honouring the parameter) teams and channels; see
[`ignore_error` scope differs by DataStore](#ignore_error-scope-differs-by-datastore) above for the
exact set per DataStore. It has no effect on permission lookups for an individual document. Those
are governed solely by `permission_failure_policy`.

#### Re-crawling after upgrading to this fix

This fix changes which ACL gets indexed, not just how failures in retrieving it are handled, so
documents indexed by an older version keep their old ACL until they are re-crawled:

- For OneDrive, SharePoint document libraries, SharePoint lists, and SharePoint pages, a user or
  group named in an ACL entry used to resolve only to an internal object ID, which matches no
  Fess role; the UPN (for users) and group name (for groups) roles that should have been added
  alongside it were silently dropped. They are added now, so re-crawled documents typically gain
  roles rather than lose them. The group name added is the group's `mail` attribute if it has
  one, otherwise `mailNickname`, otherwise `displayName` - so it is usually an email address, not
  the group's display name. If a Fess role mapping was built around the group's display name,
  check it against whichever of these attributes the group actually has.
- SharePoint page ACLs used to be indexed as raw display names (e.g. `John Doe`), which also
  match no Fess role and made the ACL inert - it neither granted nor denied access on its own
  strength. Page ACLs now carry the site's permissions in the same encoded role format as every
  other DataStore, so this is the first release where they have any effect.
- OneNote site and group notebooks (`site_note_crawler` and `group_note_crawler`, both enabled
  by default) were not indexed by any earlier release at all. A client-side bug sent every
  notebook, section, page, and page-content request for them to the wrong Graph path; Graph
  returned `404` for all of it, and that was logged only at `debug` level, so the crawl reported
  success while indexing zero notebooks. This is not an ACL correction like the ones above - it
  is the first time these notebooks are indexed at all. Site notebooks previously carried an
  unconditionally empty ACL; they now carry `default_permissions` when it is configured (see
  [What ACL Each DataStore Can Produce](#what-acl-each-datastore-can-produce) above) -
  `permission_failure_policy` still has no effect on OneNoteDataStore, because it never calls a
  Graph permission-fetch endpoint for any notebook type. A `404` that persists after re-crawling
  for a site's or group's notebooks is
  now logged at `WARN` and means the site or group genuinely has no notebooks, not that the
  crawler is asking the wrong Graph path; a user's `404` stays at `debug` (see "404 Visibility"
  below).

Re-crawl the OneDrive, SharePoint document library, SharePoint list, and SharePoint page
crawlers after upgrading to pick up the corrected ACLs, and re-crawl OneNoteDataStore (with
`site_note_crawler` and/or `group_note_crawler` enabled) to index the site and group notebooks
for the first time.

#### Re-crawling after upgrading to the crawl filter fixes

A separate set of fixes corrects which lists, items, and document libraries a crawl selects in
the first place, not just their ACLs, so a re-crawl is needed here too:

- **`list_template_filter`** (SharePointListDataStore) had two independent bugs. First, the
  numeric IDs this README always documented (e.g. `list_template_filter=100,101`) never matched
  anything, because the filter only compared against Graph's string template names internally -
  crawls configured this way indexed **zero items**, regardless of what lists existed. Second,
  even a filter that did match a list (by template name, or now by numeric ID too) only decided
  which *lists* were considered; a separate, unconditional check further down discarded every item
  whose list template wasn't `genericList`, so selecting e.g. `documentLibrary` still indexed no
  items. Both are fixed now: numeric IDs are accepted, and the filter governs item processing too.
  Re-crawl any configuration that sets `list_template_filter` to anything other than the default to
  pick up items that were previously discarded either way. The default (filter unset) is
  unchanged - generic lists only. This can also go the other way when `list_id` is set to crawl a
  single list directly: that path never consults `list_template_filter` to decide *whether* to
  crawl the list (only `list_id` does), but it now goes through the same, newly-enforced
  item-level filter as every other list. A `list_id` whose list's actual template does not match
  `list_template_filter` used to index all of that list's items regardless of the filter, and now
  indexes **zero**. If you started using `list_id` as a workaround for `list_template_filter`
  matching nothing in "all lists" mode, double-check that the filter value you left in place still
  matches the template of the list `list_id` points at.
- **`ignore_system_libraries`** was already enforced in SharePointDocLibDataStore before this
  release - nothing changes there, and no re-crawl is needed on its account. The bug was isolated
  to OneDriveDataStore: when it crawls all SharePoint sites' document libraries (Crawling Mode 1,
  which runs whenever `shared_documents_drive_crawler=true`, the default), the same check existed
  but was only ever passed as an argument to a `debug` log statement, so the default `true` changed
  nothing there - `_catalogs`, `Forms`, Style Library, and `FormServerTemplates` were crawled like
  any other library. It is actually enforced in OneDriveDataStore's Mode 1 now too, still the
  default. Because Mode 1 indexes the files inside those libraries, not just library metadata the
  way SharePointDocLibDataStore does, the drop in indexed items after a re-crawl can be far larger
  here. Re-crawl OneDriveDataStore to remove them from the index, or set
  `ignore_system_libraries=false` first if you want to keep indexing them.
- **`include_pattern` / `exclude_pattern`** (SharePointDocLibDataStore) were declared as constants
  but never read anywhere, so configuring either one had no effect at all. They now filter document
  libraries by their canonical URL (`doclib.url`) - see
  [SharePoint Document Library Parameters](#sharepoint-document-library-parameters) below for what
  that means in practice. If you had already configured either parameter for this DataStore
  expecting it to work, re-crawl to have it apply for the first time.

#### Graph client timeouts and retries

`connect_timeout`, `read_timeout`, and `access_timeout` (OkHttp's *call* timeout - the ceiling on
the whole request: DNS, connecting, writing, server processing, and reading the response, with
any redirects or retries all counted against the one period) configure the OkHttp client the
Microsoft Graph Java SDK builds internally. That client is built by the SDK's own
`GraphClientFactory`, which hard-codes all three to **100 seconds** regardless of whether this
plugin passes it any options - this is the Graph client library's own default, not OkHttp's raw
10-second connect/read default (OkHttp's call timeout has no default at all - `0`, meaning
unlimited). `0`, or leaving one of these parameters unset, keeps that 100-second default.

Setting one above `2147483` seconds - the largest whole-second value OkHttp's client builder
accepts - logs a `WARN` and uses `2147483` instead of failing. A non-numeric value also falls back
to the 100-second default, with a `WARN` logged. A negative value is treated the same as `0` (the
100-second default is kept), and now also logs a `WARN` naming the parameter and the value, so an
`access_timeout=-1` meant as "no timeout" is not silently ignored.

`max_retry_count` (default `3`, maximum `10`) and `retry_interval` (default `3` seconds, maximum
`180` seconds) configure the SDK's retry handler; which responses get retried is not
configurable. A value above the maximum, or a non-numeric value, logs a `WARN` and falls back to
the maximum or the default respectively. A negative value also logs a `WARN`, but falls back to
`0` - not to the parameter's own default of `3` - so a negative `max_retry_count` disables
retries entirely rather than reverting to three attempts, and a negative `retry_interval` removes
the delay between them.

`access_timeout` is not a new parameter: the constant has been declared since the plugin's first
release and was never read anywhere, so setting it had no effect. This release is the first time
it does anything - it now sets OkHttp's call timeout - so if you already have `access_timeout`
configured, upgrading changes its behavior from a no-op to an active timeout.

#### `additionally_allowed_tenants` and the Graph host allowlist

The credential backing this client used to accept tokens for any Azure AD tenant
(`additionallyAllowedTenants("*")`, hard-coded). It now accepts tokens only for the configured
`tenant` unless `additionally_allowed_tenants` says otherwise. Set
`additionally_allowed_tenants=*` to restore the old behavior, or list specific tenant IDs
separated by commas (surrounding whitespace and empty entries are ignored) to allow just those.
No code path in this plugin was found that requests a token for a tenant other than the
configured one, which is why the default was changed - "no path was found", not "no path exists".
An installation that does something unusual with the underlying credential outside this plugin's
own code could still depend on the old, unrestricted behavior.

Separately, a deployment that configures `proxy_host`/`proxy_port` used to send the bearer token
to any host named by a Graph response's `@odata.nextLink`, even one outside the six Microsoft
Graph national-cloud hosts. A deployment without a proxy was never affected - it already carried
that same six-host restriction. The proxied path now gets it too: a `@odata.nextLink` naming a
host outside that list is still followed, but without the `Authorization` header, rather than
leaking the tenant's app-only token to it.

#### What the two crawl-completion `ERROR` messages mean

Every DataStore in this plugin now shuts down its crawling thread pool through one shared helper,
which can log two new kinds of `ERROR` on a code path that previously said nothing at all. In this
project, `ERROR` from `org.codelibs` is wired to notifications, so upgrading can mean a crawl
configuration that looked clean starts producing notifications - the failures documented below
were already happening; only the reporting is new. Both are bounded at one line per crawl.

- `<name>: N crawling task(s) were still running and M had not started after T seconds. They are
  about to be cancelled, and the documents they would have produced are missing from this crawl.
  Raise executor_shutdown_timeout for a large tenant.` - the wait configured by
  `executor_shutdown_timeout` expired while tasks were still queued or in flight; those tasks are
  cancelled and their documents never get produced. Before this release, the same expiry cancelled
  those tasks just as silently, and the crawl reported success anyway. Raising
  `executor_shutdown_timeout` is the direct fix; `number_of_threads` is the other lever, but note
  that it is capped (see below), and the effective value sets both the pool size and the queue
  capacity - so at most `2 x` the *effective* thread count can be running or queued when the wait
  begins.
- `<name>: N crawling task(s) failed; their documents are missing from this crawl. See the warnings
  above.` - a count of tasks that ended by throwing, logged once per crawl. Each individual failure
  it counts is logged separately at `WARN`, as `<name>: a crawling task failed.`, one line per
  failed task - `WARN` rather than `ERROR` because `SharePointListDataStore` and
  `SharePointPageDataStore` submit one task per document, so a bad crawl would otherwise raise one
  notification per document. This `ERROR` is the notification, and there is at most one of it per
  crawl. Its count is a snapshot taken as the shutdown ends, and on a timed-out shutdown the tasks
  being cancelled can still go on to fail after it is read, so it can undercount; the `WARN` lines
  are the complete record. Before this release, an escaped failure either vanished with no log line
  at all, or - when the pool's queue was already full - ran on the submitting thread instead and
  propagated out of the crawl itself, aborting it outright; which of the two happened depended on
  queue timing, not on anything the operator controlled.

Both messages, and the per-task failure line, name the concrete DataStore by its Java class name
(e.g. `OneDriveDataStore`), so an operator running several Microsoft 365 crawls in one Fess
instance can tell which one produced it.

#### `number_of_threads` is capped

The requested value is capped to `min(number_of_threads, availableProcessors() x 2)`, and the
capped value sets both the pool size and the queue capacity. On a 4-core host, `number_of_threads`
of 32 gives 16 threads and a 16-deep queue, not 32 and 32; raising the parameter past the cap has
no effect at all. The cap is logged at `DEBUG` when it applies.

### Teams-Specific Parameters

| Parameter | Description | Default | Notes |
|-----------|-------------|---------|-------|
| `team_id` | Specific team ID to crawl | All teams | Microsoft 365 group ID |
| `exclude_team_ids` | Comma-separated team IDs to exclude | - | Multiple teams to skip |
| `include_visibility` | Team visibility levels to include | All | Comma-separated: `public`, `private` |
| `channel_id` | Specific channel ID to crawl | All channels | Within specified team |
| `chat_id` | Specific chat ID to crawl | - | For 1:1 or group chats |
| `ignore_replies` | Skip reply messages | `false` | Process only root messages |
| `append_attachment` | Include attachments in content | `true` | Append attachment text to message body |
| `ignore_system_events` | Skip system event messages | `true` | Filter out system notifications |
| `title_dateformat` | Date format for message titles | `yyyy/MM/dd'T'HH:mm:ss` | Java date pattern |
| `title_timezone_offset` | Timezone offset for message titles | `Z` | e.g., `Z`, `+09:00`, `-05:00` |
| `start_date` | Inclusive earliest message timestamp to index | - | `yyyy-MM-dd` (UTC start of day) or an ISO-8601 date-time **carrying an explicit offset or `Z`**, e.g. `2026-01-01` or `2026-01-01T09:00:00+09:00`. A zone-less `2026-01-01T09:00:00` is rejected. An unparseable value is logged and ignored |
| `end_date` | Inclusive latest message timestamp to index | - | `yyyy-MM-dd` (UTC end of day, `23:59:59.999999999Z`) or an ISO-8601 date-time carrying an explicit offset or `Z`. An unparseable value is logged and ignored |
| `number_of_threads` | Number of processing threads | `1` | Concurrent message processing |
| `default_permissions` | Default role assignments | - | Additional permissions for all messages |
| `ignore_error` | Continue crawling when a team or channel cannot be processed | `false` | When `true`, an unresolvable `team_id` or `channel_id`, a team whose channels cannot be listed, and a channel whose messages cannot be fetched are logged and skipped instead of aborting the crawl. Failures while enumerating **all** teams were already skipped and are unaffected; a `team_id` listed in `exclude_team_ids` that cannot be resolved still aborts, so that a team you asked to exclude is never silently crawled |

**How the Teams date range is applied**

**The filtering is client-side.** `start_date` and `end_date` are applied **after** the messages
are fetched. Microsoft's `channel: list messages` API supports only `$top` and `$expand` - "The
other OData query parameters aren't currently supported" - so there is no server-side date filter
to use. Every message in every crawled channel is still retrieved from Microsoft Graph and every
page is still walked; the range reduces what is **indexed**, not what is **transferred**. Setting
`start_date` to shorten a crawl does not work: it saves no Graph requests, no bandwidth and
(except for the reply listings noted below) no time.

The bound is compared against the message's `createdDateTime`, falling back to
`lastModifiedDateTime` when that is absent. A message with neither timestamp is indexed rather
than dropped: a missing timestamp must never be a reason to silently shrink the index. A timestamp
Microsoft Graph returns in a form the SDK cannot parse never reaches the range check at all - it
fails while the message list is being deserialized, which is a channel-level failure governed by
`ignore_error`, not by these two parameters.

Both root messages and replies are tested against the range. Because replies are only fetched for
a root message that was itself indexed, a root outside the range also excludes its replies - even
a reply that would have fallen inside it. This keeps a reply from ever being indexed with a
`parent` that was never processed, and it is the one place the range does save Graph traffic. A
reply is always at or after its root, so an `end_date` that excludes a root correctly excludes its
replies; only `start_date` can drop an in-range reply, and only from a conversation whose opening
message you asked not to index.

**`chat_id` is judged across the whole conversation.** A chat is consolidated into a single
document, so the range is evaluated once for the chat rather than per message, and the decision is
all-or-nothing: the entire conversation is indexed or none of it is, never a subset. The test is
whether **any** of the chat's messages falls inside the range - not the consolidated document's own
timestamp. That timestamp is inherited from whichever message Graph happened to return first (the
listing sets no `$orderby` and is not sorted afterwards), so judging by it would drop a chat
spanning years on the strength of one arbitrary message.

Two consequences follow from that shape:

- The indexed body is the **whole, unfiltered** conversation. Consolidating a chat into one
  document leaves no way to index part of it, so a range does not trim a chat's contents - it only
  decides whether the chat is indexed.
- `message.created_date_time` on that document is unchanged by the range. It is still the inherited
  timestamp described above, and it does **not** tell you which message put the chat in range.

A malformed `start_date` or `end_date` is logged once at `WARN` when the crawl starts and then
treated as unset, so that bound simply does not filter. The same treatment is applied to an
**inverted** range - a `start_date` later than the `end_date`, which would match no message at all:
one `WARN` at crawl start, and then **both** bounds are dropped, so the crawl runs completely
unfiltered rather than reporting success over an empty index. Equal bounds are a legitimate
one-instant range and do not warn, and a lone bound cannot be inverted, so it does not either.

A typo therefore gives you the unfiltered crawl you had before the parameter existed - it never
aborts the crawl and never silently empties the index. With both parameters unset - the default -
nothing is filtered and behaviour is unchanged.

#### Re-crawling after upgrading to the Teams fixes

Two changes in this release alter what a Teams crawl produces. Both need a re-crawl, and the
second needs a review before that re-crawl.

- **Teams crawling indexed zero documents before this release.** `getTeams` enumerated `/teams`,
  fetched each team's backing `Group`, and then checked whether that group's
  `resourceProvisioningOptions` contained `"Team"` - reading the value out of
  `Group#getAdditionalData()`. In Microsoft Graph SDK v6 `resourceProvisioningOptions` is a
  **typed** property on `Group`, so Kiota registers a field deserializer for it and routes it to
  the setter; it never reaches the additional-data map, and the two are disjoint. The check
  therefore rejected **every** team, the consumer was never called, and the crawl reported success
  while indexing nothing. The only trace was one `DEBUG` line per skipped team. The typed accessor
  is read now, and the check rejects a group only when the list is present and lacks `"Team"`.
  **Expect the indexed document count to jump from zero on the first crawl after upgrading** - that
  is the fix working, not a runaway crawl, so do not kill the job for it. Budget for the full
  volume of every team's channel messages.

  Note also that **archived teams are crawled**. The old check was named `isActiveTeam`, which
  implied it excluded them; it never looked at `Team#getIsArchived`, and neither does the current
  code. Use `exclude_team_ids` to skip archived teams you do not want indexed.

- **The data config's Permissions field now reaches Teams message ACLs.** TeamsDataStore was the
  only one of the six data stores that never folded `defaultDataMap`'s role entry into a document's
  role list, so a Teams data config's Permissions field was silently discarded for every message it
  indexed. It is applied now, in the same order the other five use: channel or chat membership,
  then `default_permissions`, then the data config's Permissions field.

  This **widens** who can retrieve Teams messages - back to what the config asked for, but wider
  than what has actually been in the index. **Audit the Permissions field on every Teams data
  config before re-crawling.** A config whose Permissions field was cloned from another data
  store's config has been carrying that other store's audience harmlessly; after this release it
  grants it. The change is purely additive - no document loses a role - and already-indexed Teams
  documents keep their old, narrower ACL until they are re-crawled.

  It is inert for one configuration: if you removed the `role=message.roles` line from the script,
  the roles Teams computes never reach the document, and the data config's Permissions field
  already reached it directly through the default data map. Nothing changes there.

#### Teams Implementation Details

The TeamsDataStore provides comprehensive Microsoft Teams content crawling with the following capabilities:

**Core Functionality:**
- **Team-based Crawling**: Processes teams, channels, and messages hierarchically
- **Chat Support**: Crawls 1:1 and group chat conversations when chat_id is specified
- **Message Aggregation**: Consolidates chat messages into searchable conversation threads
- **Permission Mapping**: Extracts team/channel membership and maps to Fess role-based access control

**Crawling Modes:**
- **All Teams**: Leave `team_id` empty to crawl all accessible teams
- **Specific Team**: Set `team_id` to crawl only that team's channels and messages
- **Team Filtering**: Use `exclude_team_ids` to skip specific teams (comma-separated IDs)
- **Visibility Filtering**: Use `include_visibility` to filter by team visibility (public/private)
- **Specific Channel**: Set both `team_id` and `channel_id` to crawl a single channel
- **Chat Conversations**: Set `chat_id` to crawl specific chat conversations

**Content Processing:**
- **Message Title Generation**: Creates searchable titles using sender name and formatted timestamp
- **Content Extraction**: Extracts message body content (text/HTML) with proper formatting
- **Attachment Handling**: Optionally includes attachment information in message content
- **Reply Threading**: Supports crawling of reply messages with parent message context
- **System Event Filtering**: Automatically filters out system-generated messages

**Message Metadata Fields:**
The implementation extracts comprehensive message metadata including:
- Basic properties: id, subject, body, created/modified timestamps
- Sender information: from user/application details
- Conversation context: team, channel, parent message references
- Interaction data: mentions, reactions, importance level
- Rich content: attachments, hosted contents, web URLs
- Permission data: role-based access control from team/channel membership

**Performance Optimizations:**
- **Multi-threaded Processing**: Configurable thread pool for parallel message processing
- **Efficient Pagination**: Uses Microsoft Graph PageIterator for handling large message sets
- **Selective Field Expansion**: Expands only necessary fields to reduce API calls
- **Permission Caching**: Caches group membership data to optimize permission mapping

**Error Handling & Resilience:**
- **Configurable Error Handling**: `ignore_error` relaxes the four failures that abort a Teams
  crawl today - an unresolvable `team_id`, an unresolvable `channel_id`, a failure listing an
  explicitly configured team's channels, and a failure fetching a channel's messages. It does not
  change any other path: at the default `false` a Teams crawl behaves exactly as it did before the
  parameter was honoured
- **Always-tolerated failures**: when crawling **all** teams (no `team_id`), a team whose channels
  cannot be listed is logged at `WARN` and skipped regardless of `ignore_error`
- **Never-tolerated failures**: an `exclude_team_ids` entry that cannot be resolved always aborts,
  even with `ignore_error=true`; ignoring it would crawl a team you asked to exclude
- **Per-message failures**: a message that fails to index is recorded in the failure-URL list and
  the crawl continues, independent of `ignore_error`
- **Comprehensive Logging**: Debug and info level logging for monitoring progress
- **Thread Pool Management**: Proper executor service shutdown and cleanup
- **Interruption Handling**: Graceful handling of thread interruption

**Content Filtering:**
- **Reply Message Filtering**: Option to skip reply messages and process only root messages
- **Date Range Filtering**: `start_date`/`end_date` bound which messages are indexed, applied
  client-side after fetching - see "How the Teams date range is applied" above
- **System Event Filtering**: Automatic detection and filtering of system-generated events
- **No pattern matching**: `include_pattern` and `exclude_pattern` are **not** supported by
  TeamsDataStore. It reads neither one, so setting either has no effect at all - not on message
  content, not on message URLs, and not on team or channel names. See the
  [semantics table](#include_pattern--exclude_pattern-semantics-differ-by-datastore) above. Use
  `team_id`, `exclude_team_ids`, `include_visibility`, `channel_id` and the date range to narrow a
  Teams crawl

**Use Cases:**
- **Team Communication Search**: Find conversations across teams and channels
- **Knowledge Discovery**: Search team discussions for solutions and decisions
- **Compliance Monitoring**: Index team communications for compliance requirements
- **Chat History Search**: Search through direct and group chat conversations

**Crawling Modes**:
- **Shared Documents Drive**: Enable `shared_documents_drive_crawler` to crawl every SharePoint site's document libraries
- **User Drives**: Enable `user_drive_crawler` to crawl all licensed users' OneDrive
- **Group Drives**: Enable `group_drive_crawler` to crawl Microsoft 365 group drives
- **Specific Drive**: Set `drive_id` to crawl that drive *in addition to* whichever of the three modes above are enabled

### OneNote-Specific Parameters

| Parameter | Description | Default | Notes |
|-----------|-------------|---------|-------|
| `site_note_crawler` | Enable crawling of site notebooks | `true` | Crawls notebooks at the root SharePoint site |
| `user_note_crawler` | Enable crawling of user notebooks | `true` | Crawls personal OneNote notebooks for licensed users |
| `group_note_crawler` | Enable crawling of group notebooks | `true` | Crawls shared notebooks in Microsoft 365 groups |
| `include_pattern` | Regex a notebook name must fully match to be crawled | - | Matched against the notebook's display name with `Pattern.matches()` (full match). An invalid regex is logged and ignored |
| `exclude_pattern` | Regex a notebook name must not fully match to be crawled | - | Matched against the notebook's display name with `Pattern.matches()` (full match). An invalid regex is logged and ignored |
| `number_of_threads` | Number of processing threads | `1` | Controls concurrent notebook processing |

If `include_pattern` or `exclude_pattern` is configured and, across all enabled scopes combined,
it admits zero of the notebooks the crawl actually saw, the crawl still finishes normally but logs
one `WARN` summarizing that - a hint that the pattern may be misconfigured, since the same crawl
otherwise reports success while indexing nothing.

#### Failure URL rows for notebooks are keyed differently

A notebook that fails to index is recorded in Fess's Failure URL admin screen. That row used to be
keyed by the notebook's **display name**, which is not unique: two notebooks sharing a name
collapsed into one row, and an operator saw one failure where there were two. The row is now keyed
by the notebook's own web URL, falling back to its id when the failure happened before that URL was
read, and to the display name only when the notebook has neither.

Nothing needs re-crawling for this. The visible effects are that failure rows for notebooks are now
followable links rather than bare names, and that a crawl with several same-named notebooks failing
reports the true count instead of one. Any existing saved filter or report keyed on the old
display-name rows will no longer match.

#### OneNote Implementation Details

The OneNoteDataStore provides comprehensive OneNote notebook crawling with the following implementation features:

**Core Functionality:**
- **Multi-Source Notebook Crawling**: Processes notebooks from three distinct sources in a systematic order
- **Aggregated Content Extraction**: Consolidates all sections and pages within each notebook into searchable content
- **Permission Mapping**: User and group notebooks get a role synthesized from the owner's ID,
  plus `default_permissions` when configured. Site notebooks carry no per-user or per-group role
  at all - Microsoft Graph exposes no app-only way to read a SharePoint site's user and group role
  assignments (see [What ACL Each DataStore Can Produce](#what-acl-each-datastore-can-produce) in
  the permissions section) - so `default_permissions` is their only role source; leave it unset
  and site notebooks are indexed but findable by nobody.

**Crawling Modes (Processing Order):**
1. **Site Notebooks**: Crawls notebooks at the root SharePoint site level (`/sites/root/onenote/notebooks`)
2. **User Notebooks**: Iterates through all licensed users and crawls their personal notebooks (`/users/{userId}/onenote/notebooks`)
3. **Group Notebooks**: Crawls shared notebooks associated with Microsoft 365 groups (`/groups/{groupId}/onenote/notebooks`)

**Content Processing Pipeline:**
1. **Notebook Discovery**: Uses Microsoft Graph API to enumerate notebooks based on enabled crawling modes
2. **Section Traversal**: For each notebook, retrieves all sections within it
3. **Page Content Extraction**: For each section, fetches all pages and extracts their HTML content
4. **Content Aggregation**: Combines all page content using Tika to extract plain text from HTML
5. **Metadata Enrichment**: Captures notebook metadata including creation/modification times and access URLs

**Configuration Flexibility:**
- **Selective Crawling**: Enable/disable specific notebook sources independently
- **Boolean Parameter Handling**: Case-insensitive boolean values (`true`, `True`, `TRUE`, `false`, `False`, `FALSE`)
- **Invalid Value Handling**: Invalid boolean values default to `false` for safety
- **Null Value Handling**: Null or missing parameters use default values (all crawlers enabled by default)

**Performance Optimizations:**
- **Concurrent Processing**: Configurable thread pool for parallel notebook processing
- **Efficient API Usage**: Batches API calls where possible to reduce Graph API quota consumption
- **Content Size Tracking**: Monitors and reports content size for each notebook

**Error Handling & Resilience:**
- **Graceful Degradation**: Handles invalid parameter values by defaulting to safe configurations
- **Thread Pool Management**: Proper executor service lifecycle management with shutdown handling
- **Comprehensive Logging**: Debug-level logging for monitoring crawling progress and troubleshooting
- **404 Visibility**: A `404` when listing a site's or group's notebooks is logged at `WARN`, not
  `debug` - it usually means that site or group has no notebooks, but the same response is also
  what a request sent to the wrong Graph path would return, so it is worth seeing. A `404` for a
  *user's* notebooks stays at `debug`: an unprovisioned personal site 404s there routinely for any
  tenant with unlicensed-for-OneDrive or never-logged-in users, and the user path was never the
  one this fix repaired, so logging one `WARN` per such user would add volume without adding
  diagnostic value.
- **Site Permission Failures**: A failure to resolve the site's ACL for site notebooks (governed
  by `permission_failure_policy`) only skips the site notebooks; user and group notebook crawling
  continues regardless

**Content Metadata Fields:**
The implementation extracts and indexes the following notebook metadata:
- `notebook.name`: The display name of the notebook
- `notebook.contents`: Aggregated text content from all sections and pages
- `notebook.size`: Total size of the extracted content in characters
- `notebook.created`: Notebook creation timestamp
- `notebook.last_modified`: Last modification timestamp
- `notebook.web_url`: Direct link to open the notebook in OneNote
- `notebook.roles`: Users/groups with access permissions

**Use Cases:**
- **Knowledge Base Search**: Search across organizational OneNote documentation
- **Personal Note Discovery**: Find information in personal OneNote notebooks
- **Team Collaboration Search**: Search shared team notebooks for meeting notes and project documentation
- **Cross-Platform Content**: Index OneNote content created from web, desktop, and mobile applications

### OneDrive-Specific Parameters

| Parameter | Description | Default | Notes |
|-----------|-------------|---------|-------|
| `max_content_length` | Maximum file size in bytes. Checked against the drive item's Graph-reported size before download, and again by the extractor against the bytes read | `-1` (defer to Fess's per-MIME-type limit, **not** unlimited) | Files over the cap are not indexed at all - see [`max_content_length` is applied twice](#max_content_length-is-applied-twice) |
| `ignore_folder` | Skip folder documents | `true` | Process files only, ignore folders |
| `supported_mimetypes` | Supported MIME types pattern | `.*` | Regex pattern for supported file types |
| `drive_id` | Additional specific drive ID to crawl | - | Adds a fourth crawl of that one drive. It does **not** restrict the others: the three `*_drive_crawler` modes still run according to their own flags (all default to `true`), so setting only `drive_id` crawls that drive *in addition to* everything else. To crawl one drive and nothing else, also set `shared_documents_drive_crawler`, `user_drive_crawler` and `group_drive_crawler` to `false` |
| `shared_documents_drive_crawler` | Enable SharePoint document library crawling | `true` | Enumerates every SharePoint site and crawls the files in its document libraries. It does **not** crawl the signed-in user's own OneDrive - no `/me/drive` request is ever issued by this DataStore |
| `user_drive_crawler` | Enable user drives crawling | `true` | Crawl all licensed users' drives |
| `group_drive_crawler` | Enable group drives crawling | `true` | Crawl Microsoft 365 group drives |
| `ignore_system_libraries` | Skip system libraries (`_catalogs`, `Forms`, Style Library, `FormServerTemplates`) | `true` | Applies whenever `shared_documents_drive_crawler=true` (default), to the sub-mode that enumerates all SharePoint sites' document libraries (Crawling Mode 1 below) - independent of `drive_id`. Setting `drive_id` runs an additional, separate crawl (Crawling Mode 4) that does not go through this check; it does not turn off Mode 1. Has no effect on personal or group drives. Matched case-insensitively against the drive's URL, same as [SharePoint Document Library Parameters](#sharepoint-document-library-parameters) below - so a site whose path merely contains a `/Forms/` segment (e.g. a site collection named "Forms") is misdetected as a system library and has all of its files skipped by default, not just an actual Forms system library |

#### The per-item failure log line changed

`processDriveItem` has two catch arms, and both used to log `Crawling Access Exception at : {}`.
OneDriveDataStore was the only one of the six data stores whose two failure paths could not be told
apart in the crawler log. The second arm - everything that is not a `CrawlingAccessException` - now
logs `Processing exception at : {}`, the phrasing the other five stores already use.

Both stay at `WARN`, both still record a failure-URL row, and the failure-URL rows are unchanged
(they are already keyed by error class). Only the log text differs. **An alert or log filter
grepping for `Crawling Access Exception at` will stop matching non-access failures on OneDrive**;
match `Processing exception at` as well, or drop to matching the shared `at : ` suffix.

#### OneDrive Implementation Details

The OneDriveDataStore provides comprehensive Microsoft 365 file crawling capabilities with the following implementation features:

**Core Functionality:**
- **Multi-Drive Type Support**: Processes files from OneDrive personal drives, SharePoint document libraries (via Drive API), and Microsoft 365 group drives
- **Hierarchical File Traversal**: Recursively crawls drive items starting from root, handling both files and folders with proper parent-child relationships
- **Content Extraction & Indexing**: Each file becomes a searchable entity with extracted content, metadata, and permission information
- **Permission Integration**: Extracts and maps Microsoft 365 access permissions to Fess role-based access control

**Crawling Modes (Processing Order):**
1. **Shared Documents Drive**: Enumerates every SharePoint site (`GET /sites`) and crawls the files in each site's document libraries (honoring `ignore_system_libraries`, see the parameters table above). Despite the mode's name it never touches the signed-in user's own OneDrive - the code has no `/me/drive` call path at all. Runs whenever `shared_documents_drive_crawler=true` (default), regardless of whether `drive_id` is also set for Mode 4 below - the two crawls run independently, not exclusively
2. **User Drives**: Iterates through all licensed users and crawls their personal OneDrive (`/users/{userId}/drive`)
3. **Group Drives**: Crawls Microsoft 365 group-associated drives (`/groups/{groupId}/drive`)
4. **Specific Drive**: Targets a single drive by ID when `drive_id` parameter is specified (`/drives/{driveId}`)

**Content Processing Pipeline:**
1. **Drive Discovery**: Uses Microsoft Graph API to enumerate drives based on enabled crawling modes and site/drive access
2. **Item Enumeration**: Retrieves drive items using pagination with `DriveItemCollectionResponse` and `@odata.nextLink` handling
3. **Content Filtering**: Applies MIME type filtering, file size limits, and include/exclude patterns before processing
4. **Content Extraction**: Uses Tika extractor with configurable name (`extractorName`, default: "tikaExtractor") for text extraction from supported file types
5. **Metadata Enrichment**: Extracts comprehensive file metadata including timestamps, permissions, and parent folder information
6. **URL Generation**: Creates user-friendly URLs based on crawler type and SharePoint/OneDrive location patterns

**Performance Optimizations:**
- **Concurrent Processing**: Configurable thread pool (`number_of_threads`) for parallel processing of multiple drives and files
- **Efficient Pagination**: Handles Microsoft Graph API pagination using `@odata.nextLink` with helper methods
- **Smart Filtering**: Pre-filters items by MIME type patterns and file size before expensive content extraction
- **Identity Caching**: The Graph client caches the user/group lookups this DataStore's ACL building repeats - see [`cache_size` and the client's lookup caches](#cache_size-and-the-clients-lookup-caches). There is no drive-ID cache: earlier releases of this document described one (`cachedUserDriveId`/`driveIdCacheLock`), but it had no callers and has been removed

**Error Handling & Resilience:**
- **Configurable Error Tolerance**: `ignore_error` relaxes exactly one failure here - a file whose content cannot be extracted is indexed with empty contents instead of failing. It does not affect item enumeration; see [`ignore_error` scope differs by DataStore](#ignore_error-scope-differs-by-datastore)
- **Exception Classification**: Differentiates between access exceptions and general exceptions for appropriate error handling
- **Failure URL Tracking**: Integration with Fess failure URL service for monitoring and retry capabilities
- **Comprehensive Logging**: Debug-level logging for detailed crawling progress monitoring and troubleshooting

**Content Metadata Extraction:**
The implementation extracts and indexes 30+ metadata fields per file:
- **Basic Properties**: name, description, size, MIME type, file type, creation/modification timestamps
- **Location & Access**: web URLs, WebDAV URLs, processed URLs for SharePoint navigation
- **Version Control**: ETag, CTag for change detection and synchronization
- **Creator Information**: user, application, and device details for created/modified by tracking
- **Rich Metadata**: image/photo/video properties, geographic location data, file hash values
- **Folder Structure**: parent reference information including path, name, and ID
- **Specialized Data**: publication info, search result metadata, special folder classification
- **Permission Data**: role-based access control extracted from Microsoft Graph permissions API

**URL Processing Strategy:**
The implementation generates user-friendly URLs based on crawling context:
- **SharePoint Libraries**: `{siteUrl}/Shared%20Documents/{path}` for shared/group drives
- **OneDrive Personal**: `{siteUrl}/Documents/{path}` for user drives
- **Custom Drives**: `{siteUrl}/{driveName}/{path}` for specific drive crawling
- **URL Encoding**: Proper encoding of file and folder names with space handling

**Content Size Management:**
- **Configurable Limits**: `max_content_length` parameter with fallback to Fess content length helper
- **MIME Type Support**: Regex pattern matching for `supported_mimetypes` (default: all types)
- **Folder Handling**: Optional folder document creation controlled by `ignore_folder` parameter
- **Size Validation**: Pre-extraction validation to avoid processing oversized files

### SharePoint Document Library Parameters

| Parameter | Description | Default | Notes |
|-----------|-------------|---------|-------|
| `site_id` | Specific site ID to crawl | All sites | Full site ID format: `hostname,siteCollectionId,siteId` |
| `exclude_site_id` | Site IDs to exclude | - | See format guide below |
| `ignore_system_libraries` | Skip system libraries | `true` | Excludes `_catalogs`, `Forms`, Style Library, and `FormServerTemplates` folders (matched case-insensitively against the drive's URL) |
| `number_of_threads` | Number of processing threads | `1` | Concurrent document library processing |
| `ignore_error` | Continue crawling on errors | `false` | Set to `true` to skip failed libraries |
| `include_pattern` | Regex pattern matched against the library's **canonical URL** (`doclib.url`), not its name | - | e.g. `https://contoso\.sharepoint\.com/sites/allowed/.*` |
| `exclude_pattern` | Regex pattern matched against the library's **canonical URL** (`doclib.url`), not its name | - | e.g. `.*/sites/blocked/.*` |
| `default_permissions` | Default role assignments | - | Additional permissions for all libraries |

> **Behavior changes in this release:**
> - `ignore_system_libraries` (default `true`) already worked correctly in this DataStore before
>   this release: the check was a real conditional here, not just logged, so nothing changes for
>   SharePointDocLibDataStore and no re-crawl is needed on its account. The bug this release fixes
>   was isolated to OneDriveDataStore, where the same check was only ever passed as an argument to
>   a `debug` log statement - see
>   [Re-crawling after upgrading to the crawl filter fixes](#re-crawling-after-upgrading-to-the-crawl-filter-fixes)
>   above for what changes there.
> - `include_pattern` / `exclude_pattern` were declared but never referenced anywhere in this
>   DataStore, so configuring them previously did nothing. They are wired to a Fess `UrlFilter` now,
>   matched against the library's canonical URL (`doclib.url`, generated by
>   `generateDocumentLibraryUrl(site, drive)`) - **not** the library's display name and **not**
>   `drive.getWebUrl()` (`doclib.web_url`). This is the same `UrlFilter` mechanism OneDriveDataStore
>   uses, just matched against a different URL.

##### exclude_site_id Format

SharePoint site IDs contain commas as part of their format (`hostname,siteCollectionId,siteId`). To properly exclude sites:

- **Single SharePoint site**: Use the full site ID as-is
  ```
  exclude_site_id=site1.sharepoint.com,686d3f1a-a383-4367-b5f5-93b99baabcf3,12048306-4e53-420e-bd7c-31af611f6d8a
  ```

- **Multiple SharePoint sites**: Separate with semicolons (`;`)
  ```
  exclude_site_id=site1.sharepoint.com,guid1,guid1;site2.sharepoint.com,guid2,guid2
  ```

- **Legacy simple IDs**: Comma-separated (for backward compatibility)
  ```
  exclude_site_id=site1,site2,site3
  ```

#### SharePoint Document Library Implementation Details

The SharePointDocLibDataStore provides comprehensive metadata indexing for SharePoint document libraries across your organization with the following key features:

**Core Functionality:**
- **Library-Level Indexing**: Each SharePoint document library becomes a single searchable entity in the Fess index, combining library metadata with site context for enhanced discoverability
- **Site Traversal**: Supports crawling all accessible sites or targeting specific sites using the `site_id` parameter
- **System Library Filtering**: Automatically excludes system document libraries (Form Templates, Style Library, etc.) unless explicitly configured otherwise with `ignore_system_libraries` parameter
- **Permission Integration**: Extracts and maps SharePoint access permissions to Fess role-based access control

**Content Aggregation:**
The implementation creates rich, searchable content by combining:
- Document library name and description
- Parent SharePoint site name and context
- Library creation and modification timestamps
- Access permission information
- Standardized URLs for both Graph API access and user navigation

**URL Generation Strategy:**
- **Graph API URL**: Preserved from Microsoft Graph response for API compatibility (`doclib.web_url`)
- **Canonical URL**: Generated standardized SharePoint URLs for user navigation (`doclib.url`)
  - Standard "Documents" libraries: `{siteUrl}/Shared%20Documents`
  - Custom libraries: `{siteUrl}/{encodedLibraryName}` with proper URL encoding

**Multi-Threading Support:**
- Configurable concurrent processing using `number_of_threads` parameter (default: 1)
- Thread-safe execution with proper ExecutorService management and resource cleanup
- Graceful handling of thread interruption during long-running operations
- Executor shutdown waits `executor_shutdown_timeout` seconds (default 60) before cancelling whatever has not finished

**Error Handling & Resilience:**
- Comprehensive error tracking with integration into Fess failure URL service
- Configurable error handling via `ignore_error` parameter (default: false)
- Detailed logging for monitoring and troubleshooting crawling operations
- Statistical tracking for performance monitoring and reporting using CrawlerStatsHelper
- Distinction between CrawlingAccessException and general exceptions for appropriate handling

**Performance Optimizations:**
- Efficient site and drive enumeration using Microsoft Graph API with pagination support
- Drive type filtering to process only document library drives (`documentLibrary` type)
- Parallel processing of multiple document libraries within sites using ExecutorService
- Memory-efficient processing with proper resource management and cleanup

**Configuration Flexibility:**
- **Site Exclusion**: Advanced `exclude_site_id` parameter supporting both simple comma-separated IDs and complex SharePoint site ID format with semicolon separation
- **Pattern Filtering**: Support for `include_pattern` and `exclude_pattern` regex filtering on the library's canonical URL (`doclib.url`), not its name - via the same Fess `UrlFilter` OneDriveDataStore uses
- **Permission Management**: Default permissions assignment via `default_permissions` parameter
- **Threading Control**: Configurable `number_of_threads` for optimal performance tuning

**Integration with Fess Security Model:**
- Automatic extraction of SharePoint permissions using Microsoft Graph API drive permissions endpoint
- Conversion of Microsoft 365 access permissions to Fess role format with proper encoding
- Support for default permission assignment via configuration parameters
- Inheritance of site-level permissions for document libraries with pagination support for large permission sets

**Use Cases:**
- **Document Library Discovery**: Find and access specific SharePoint document libraries across the organization
- **Content Organization**: Search for document libraries by name, description, or site context
- **Permission Auditing**: Identify document libraries and their access permissions
- **Site Navigation**: Discover available document libraries within SharePoint sites

**Important Note**: This data store focuses on document library metadata indexing. For indexing individual files within SharePoint document libraries, use the OneDriveDataStore which handles SharePoint document library files through the Microsoft Graph Drive API.

### SharePoint List Parameters

| Parameter | Description | Default | Notes |
| --- | --- | --- | --- |
| `site_id` | SharePoint site ID containing lists | Required | Full site ID format: `hostname,siteCollectionId,siteId` |
| `list_id` | Specific list ID to crawl | All lists | If specified, only this list will be crawled |
| `exclude_list_id` | Comma-separated list IDs to exclude | - | Multiple list IDs separated by commas |
| `list_template_filter` | Filter which lists - and, since this fix, which of their items - are processed, by template type | - | Comma-separated numeric IDs and/or Graph template names, e.g. `100,101` or `genericList,documentLibrary`; see [List Template Types](#list-template-types) below |
| `ignore_system_lists` | Skip system lists | `true` | Excludes lists like User Information, Workflow Tasks |
| `ignore_error` | Continue crawling on errors | `false` | Set to `true` to skip failed items |
| `include_pattern` | Regex pattern for item titles to include | - | Filter items by title matching |
| `exclude_pattern` | Regex pattern for item titles to exclude | - | Skip items with matching titles |
| `number_of_threads` | Number of processing threads | `1` | Concurrent list processing |
| `default_permissions` | Default role assignments | - | Additional permissions for all items |

#### SharePoint List Implementation Details

The SharePointListDataStore provides comprehensive crawling and indexing of SharePoint lists and list items with the following capabilities:

**Core Functionality:**
- **List Item Indexing**: Each SharePoint list item becomes a searchable entity with dynamic field extraction and content aggregation
- **Site-Specific Crawling**: Requires a `site_id` parameter to target lists within a specific SharePoint site
- **List Filtering**: Supports crawling all lists or specific lists using `list_id`, with exclusion capabilities via `exclude_list_id`
- **Template-Based Filtering**: Filter which lists - and which of their items are processed - by SharePoint template type, using numeric IDs or Graph template names (e.g. `100`/`genericList` for Generic Lists, `101`/`documentLibrary` for Document Libraries). Without this filter, only generic-list items are processed (unchanged default behavior); setting it also opens up processing of items in the selected template(s), which previously never happened regardless of this filter
- **System List Exclusion**: Automatically skips system lists unless explicitly configured otherwise

**Content Extraction Strategy:**
The implementation intelligently extracts content from list items:
- **Title Extraction**: Searches for title in common fields (Title, LinkTitle, FileLeafRef)
- **Content Building**: Aggregates text from content fields (Body, Description, Comments, Notes)
- **Dynamic Field Mapping**: Captures all custom SharePoint fields in the `item.fields` map
- **Field Expansion**: Automatically expands field data if not initially available via `$expand=fields`
- **System Field Filtering**: Excludes internal SharePoint system fields from content aggregation

**Multi-Threading Support:**
- Configurable concurrent processing using `number_of_threads` parameter
- Thread pool management with proper resource cleanup
- Parallel processing of multiple lists and list items
- Graceful handling of thread interruption

**Error Handling & Resilience:**
- **Failure Tracking**: Integration with Fess failure URL service for error monitoring
- **Configurable Error Handling**: `ignore_error` parameter controls continuation on failures
- **Statistical Tracking**: Monitors crawling progress with document counts and timing metrics
- **Comprehensive Logging**: Debug and info level logging for troubleshooting

**Permission Management:**
- Extracts SharePoint list and item permissions via Microsoft Graph API
- Maps Microsoft 365 access control to Fess role-based security model
- Supports default permission assignment through configuration
- Inherits site and list-level permissions for items

**Attachments are not supported:**
- Classic SharePoint list-item attachments are **not** indexed, and there is no script field for
  them. Microsoft Graph exposes no list-item attachments endpoint in either v1.0 or beta - the
  `listItem` resource's relationships are `activities`, `analytics`, `documentSetVersions`,
  `driveItem`, `fields`, `permissions` and `versions`. The documented API is SharePoint REST
  (`_api/web/lists/.../AttachmentFiles`), which Microsoft requires a **certificate** credential to
  call app-only; this plugin authenticates with a client secret scoped to Microsoft Graph.
- Files stored in a **document library** are indexed normally by `oneDriveDataStore` (individual
  files) and `sharePointDocLibDataStore` (library metadata).

**URL Filtering:**
- **Include Pattern**: Regex-based filtering to include specific items by title
- **Exclude Pattern**: Regex-based filtering to exclude items by title
- Efficient pattern matching with pre-compiled regex patterns

**Use Cases:**
- **Structured Data Search**: Index and search custom business data stored in SharePoint lists
- **Task and Issue Tracking**: Search across task lists, issue trackers, and project lists
- **Document Metadata**: Index document libraries managed as SharePoint lists
- **Custom Applications**: Search data from Power Apps and custom SharePoint solutions
- **Business Process Content**: Index workflow-related lists and approval items

##### List Template Types

`list_template_filter` accepts a comma-separated mix of numeric SharePoint template IDs and
Microsoft Graph template name strings - for example `list_template_filter=100,documentLibrary` is
valid, and either form of `100` or `genericList` matches the same lists. Only the IDs Microsoft
documents against Graph's `list.template` property are mapped to a name internally, so only those
IDs can be given numerically; the rest exist only as legacy `SPListTemplateType` IDs and have no
published Graph name to map to:

| ID | Name | Filter value to use |
|----|------|----------------------|
| `100` | Generic List (Custom Lists) | `100` or `genericList` |
| `101` | Document Library | `101` or `documentLibrary` |
| `102` | Survey | `102` or `survey` |
| `103` | Links | `103` or `links` |
| `104` | Announcements | `104` or `announcements` |
| `105` | Contacts | `105` or `contacts` |
| `106` | Events | name only - not published, see below |
| `107` | Tasks | name only - not published, see below |
| `108` | Discussion Board | name only - not published, see below |
| `109` | Picture Library | name only - not published, see below |

For `106`-`109`, Microsoft has not published what string value Graph's `list.template` reports, so
this plugin cannot map the numeric ID to it, and guessing would silently reintroduce the same
no-match problem this mapping exists to fix. Passing one of these IDs numerically (e.g.
`list_template_filter=106`) logs a `WARN` ("Unknown list template ID ...; use the Graph template
name instead") and matches nothing. To filter on one of these types, enable `DEBUG` logging for
`org.codelibs.fess.ds.ms365` (see [Debug Mode](#debug-mode) below), run a crawl, and read the
`Template:` value logged for that list - then use that literal string as the filter value instead
of the numeric ID.

**Performance Optimizations:**
- Efficient list enumeration with pagination support
- Lazy loading of list items with Microsoft Graph PageIterator
- Memory-efficient processing of large lists
- Caching of compiled regex patterns for filtering

### SharePoint Pages Parameters

| Parameter | Description | Default | Notes |
| --- | --- | --- | --- |
| `site_id` | SharePoint site ID containing pages | All sites | Full site ID format: `hostname,siteCollectionId,siteId` |
| `exclude_site_id` | Comma-separated site IDs to exclude | - | Multiple site IDs separated by commas |
| `ignore_system_pages` | Skip system pages | `true` | Excludes Forms, DevHome, and other system pages |
| `page_type_filter` | Filter by page type | All types | Comma-separated: `news,article,page` |
| `ignore_error` | Continue crawling on errors | `false` | Set to `true` to skip failed pages |
| `include_pattern` | Regex pattern for page URLs to include | - | Filter pages by URL matching |
| `exclude_pattern` | Regex pattern for page URLs to exclude | - | Skip pages with matching URLs |
| `number_of_threads` | Number of processing threads | `1` | Concurrent page processing |
| `default_permissions` | Default role assignments | - | Additional permissions for all pages |

**Crawling Modes**:
- **All Sites**: Leave `site_id` empty to crawl pages from all accessible sites
- **Specific Site**: Set `site_id` to crawl only pages from that site
- **Filtered Content**: Use `page_type_filter` to limit to specific page types (news, articles, etc.)

**Content Processing**: Pages are processed with canvas layout expansion to extract rich content from web parts, including text formatting and embedded data when available through the Microsoft Graph API.

#### Re-crawling after upgrading to standard web-part extraction

Standard web parts (everything except plain text web parts - Quick Links, Hero, Events, News,
and so on) previously contributed **nothing** to `page.content`: the extractor received a typed
`WebPartData` object it could not read, so it appended no characters. It now extracts the web
part's `title`, `description` and SharePoint's own indexable projection
(`serverProcessedContent.searchablePlainTexts`, `htmlStrings` and `links`).

It also walks the web part's `additionalData` map - a forward-compatibility read for any Graph
field the SDK's typed `WebPartData` model does not (yet) declare, not a source of text today. On
the pinned Graph SDK version, `WebPartData` already models every documented field except
`audiences` (a list of GUIDs), and a GUID is filtered out the same as anywhere else in this
extractor. So as of this SDK version `additionalData` contributes no text - and no noise - to
`page.content`; the read is kept because it is free once the SDK adds a new field Graph starts
populating there.

No field is renamed or removed and `page.content` never shrinks, so existing scripts and index
mappings keep working unchanged. But **existing indexes will not contain the new text until the
pages are re-crawled**, and once re-crawled those documents have longer content and therefore
different relevance scores. Re-crawl the SharePoint Pages data store after upgrading if you rely
on web-part text being searchable.

The web part's free-form `properties` object is still not read: Microsoft Graph exposes it as an
untyped node with no published schema.

## 🔧 Development

### Tech Stack

- **Language**: Java 21
- **Build Tool**: Maven 3.8+
- **Framework**: Fess Data Store (LastaFlute/DBFlute)
- **API Client**: Microsoft Graph SDK v6
- **Authentication**: Azure Identity SDK
- **Testing**: UTFlute with JUnit 4
- **Dependency Management**: Maven Shade Plugin with relocation

### Project Structure

```
src/
├── main/java/org/codelibs/fess/ds/ms365/
│   ├── Microsoft365DataStore.java        # Abstract base class
│   ├── OneDriveDataStore.java            # OneDrive implementation
│   ├── OneNoteDataStore.java             # OneNote implementation
│   ├── TeamsDataStore.java               # Teams implementation
│   ├── SharePointDocLibDataStore.java    # SharePoint doc libs
│   ├── SharePointListDataStore.java      # SharePoint lists
│   ├── SharePointPageDataStore.java      # SharePoint pages
│   └── client/
│       └── Microsoft365Client.java       # Graph API wrapper
├── main/resources/
│   └── fess_ds++.xml                     # DI configuration
└── test/java/org/codelibs/fess/ds/ms365/ # Test classes
```

### Building the Project

```bash
# Clean build
mvn clean package

# Run tests
mvn test

# Run specific test
mvn test -Dtest=OneDriveDataStoreTest

# Format code
mvn formatter:format

# Build without tests (faster)
mvn clean package -DskipTests
```

### Development Setup

1. **Prerequisites**:
   ```bash
   # Check Java version
   java -version  # Should be 21+
   
   # Check Maven version  
   mvn -version   # Should be 3.8+
   ```

2. **Clone and Setup**:
   ```bash
   git clone https://github.com/codelibs/fess-ds-microsoft365.git
   cd fess-ds-microsoft365
   
   # Install parent POM
   git clone https://github.com/codelibs/fess-parent.git
   cd fess-parent && mvn install -Dgpg.skip=true && cd ..
   
   # Build project
   mvn clean compile
   ```

3. **IDE Setup**:
   - Import as Maven project
   - Set Java 21 as project SDK
   - Enable annotation processing
   - Use the Eclipse formatter config in `src/config/eclipse/formatter/`

### Testing Strategy

The project uses UTFlute framework with mock Microsoft Graph API responses:

```bash
# Run all tests
mvn test

# Test specific data store
mvn test -Dtest=OneDriveDataStoreTest
mvn test -Dtest=Microsoft365ClientTest

# Test with debug output
mvn test -X -Dtest=SharePointDocLibDataStoreTest
```

### Contributing Guidelines

1. **Code Style**: Use the provided Eclipse formatter configuration
2. **Testing**: Write tests for new functionality using existing patterns
3. **Documentation**: Update README and JavaDocs for API changes
4. **Versioning**: Follow semantic versioning for releases
5. **Pull Requests**: Ensure CI passes before submitting

### Dependency Management

The project uses Maven Shade Plugin to bundle Microsoft Graph SDK dependencies with package relocation to avoid conflicts:

- **Relocated Packages**: `io.netty.*` → `org.codelibs.fess.ds.ms365.netty.*`
- **Bundled Libraries**: Azure SDK, Microsoft Graph SDK v6, Reactor Netty
- **Provided Dependencies**: Fess framework, OpenSearch, Jakarta APIs

## 📋 Usage Examples

### Example 1: OneDrive File Search
```javascript
// Fess search script mapping for OneDrive
title=file.name
content=file.description + "\n" + file.contents  
mimetype=file.mimetype
created=file.created
last_modified=file.last_modified
url=file.web_url
role=file.roles
```

### Example 2: SharePoint List Configuration
```properties
# SharePoint list crawling with filtering
site_id=contoso.sharepoint.com,686d3f1a-a383-4367-b5f5-93b99baabcf3,12048306-4e53-420e-bd7c-31af611f6d8a
list_template_filter=100,101  # Generic lists and Document Libraries
ignore_system_lists=true
include_pattern=.*Important.*
exclude_pattern=.*Draft.*
ignore_error=false
number_of_threads=2
default_permissions={role}sharepoint-users

# Crawl specific list only
# list_id=12345678-1234-1234-1234-123456789abc

# Exclude multiple lists
# exclude_list_id=list1-id,list2-id,list3-id
```

### Example 3: Teams Content Search
```javascript
// Teams message indexing script
title=message.title
content=message.content
created=message.created_date_time
last_modified=message.last_modified_date_time
url=message.web_url
role=message.roles
// Access additional fields
team_name=team.displayName
channel_name=channel.displayName
sender=message.from.user.displayName
```

### Example 4: Teams Configuration
```properties
# Crawl specific team with filters
team_id=12345678-1234-1234-1234-123456789abc
ignore_replies=true
ignore_system_events=true
append_attachment=true
number_of_threads=2
title_dateformat=yyyy/MM/dd'T'HH:mm:ss
title_timezone_offset=+09:00

# Exclude multiple teams
exclude_team_ids=team1-id,team2-id,team3-id
include_visibility=public,private

# Crawl specific channel in a team
# channel_id=19:channel-id@thread.tacv2

# Crawl specific chat conversation
# chat_id=19:chat-id@thread.v2
```

### Example 5: SharePoint Pages Content Search
```javascript
// SharePoint pages indexing script
title=page.title
content=page.content
created=page.created
last_modified=page.modified
url=page.url
role=page.roles
// Access additional page fields
page_type=page.type
author=page.author
site_name=page.site_name
description=page.description
```

### Example 6: SharePoint Pages Configuration
```properties
# Crawl pages from all sites with content filtering
ignore_system_pages=true
page_type_filter=news,article
include_pattern=.*important.*|.*announcement.*
exclude_pattern=.*draft.*|.*temp.*
number_of_threads=2
ignore_error=false

# Crawl pages from specific site only
# site_id=contoso.sharepoint.com,686d3f1a-a383-4367-b5f5-93b99baabcf3,12048306-4e53-420e-bd7c-31af611f6d8a

# Exclude multiple sites
# exclude_site_id=site1.sharepoint.com,guid1,guid1;site2.sharepoint.com,guid2,guid2
```

## 🔍 Troubleshooting

### Common Issues

**Authentication Errors**
```
Solution: Verify Azure AD app permissions and admin consent
- Check tenant ID format
- Ensure client secret hasn't expired
- Verify API permissions are granted
```

**Rate Limiting**
```
Solution: Adjust threading and implement backoff
- Reduce number_of_threads parameter
- Enable ignore_error to continue on throttling
- Monitor Microsoft Graph API limits
```

**Large Content Issues**
```
Solution: Configure content handling
- Implement exclude_pattern for large files
- Use OneDriveDataStore settings for file content extraction
```

### Debug Mode

Enable debug logging in Fess to troubleshoot issues:

```xml
<!-- Add to log4j2.xml -->
<Logger name="org.codelibs.fess.ds.ms365" level="DEBUG"/>
```

## 📚 Additional Resources

- **Fess Documentation**: https://fess.codelibs.org/
- **Microsoft Graph API**: https://docs.microsoft.com/en-us/graph/
- **Azure AD App Registration**: https://docs.microsoft.com/en-us/azure/active-directory/develop/
- **Issue Tracker**: https://github.com/codelibs/fess-ds-microsoft365/issues

## 📄 License

Licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.

