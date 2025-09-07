Microsoft365 Data Store for Fess
[![Java CI with Maven](https://github.com/codelibs/fess-ds-microsoft365/actions/workflows/maven.yml/badge.svg)](https://github.com/codelibs/fess-ds-microsoft365/actions/workflows/maven.yml)
==========================

## Overview

Microsoft365 Data Store is an extension for Fess Data Store Crawling that enables comprehensive crawling of Microsoft 365 services including OneDrive, OneNote, Teams, SharePoint Sites, and SharePoint Lists via Microsoft Graph API v6.

## Features

- **OneDrive**: Crawl user and group OneDrive files and folders
- **OneNote**: Crawl notebooks, sections, and pages
- **Teams**: Crawl team channels, messages, and chats
- **SharePoint Sites**: Crawl SharePoint sites and document libraries with enhanced content aggregation
- **SharePoint Lists**: Crawl SharePoint lists and list items with improved processing and statistical tracking
- **Microsoft Graph SDK v6**: Latest SDK with pagination and caching support
- **Role-based Access Control**: Integrated with Fess security model
- **Configurable Filtering**: Include/exclude patterns and system content filtering
- **Enhanced Content Fields**: Rich content aggregation for better search results
- **Robust Error Handling**: Comprehensive error tracking and failure recovery

## Download

See [Maven Repository](https://repo1.maven.org/maven2/org/codelibs/fess/fess-ds-microsoft365/).

## Installation

1. Download fess-ds-microsoft365-X.X.X.jar
2. Copy fess-ds-microsoft365-X.X.X.jar to $FESS\_HOME/app/WEB-INF/lib or /usr/share/fess/app/WEB-INF/lib

## Getting Started

### Authentication Parameters

```
tenant=********-****-****-****-************
client_id=********-****-****-****-************
client_secret=***********************
```

### Data Store Types

The plugin provides five different data store types for crawling different Microsoft 365 services:

1. **oneDriveDataStore** - OneDrive files and folders
2. **oneNoteDataStore** - OneNote notebooks and pages
3. **teamsDataStore** - Teams channels and messages
4. **sharePointSiteDataStore** - SharePoint sites and document libraries
5. **sharePointListDataStore** - SharePoint lists and list items

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
| file.contents | The text contents of the file |
| file.mimetype | The MIME type of the file. |
| file.created | The time at which the file was created. |
| file.last_modified | The last time the file was modified by anyone. |
| file.web_url | A link for opening the file in an editor or viewer in a browser. |
| file.roles | A users/groups who can access the file. |

#### OneNote

```
title=notebooks.name
content=notebooks.contents
created=notebooks.created
last_modified=notebooks.last_modified
url=notebooks.web_url
role=notebooks.roles
```

| Key | Value |
| --- | --- |
| notebooks.name | The name of the notebook. |
| notebooks.contents | The text contents of the notebook |
| notebooks.created | The time at which the notebook was created. |
| notebooks.last_modified | The last time the notebook was modified by anyone. |
| notebooks.web_url | A link for opening the notebook in an editor in a browser. |
| notebooks.roles | A users/groups who can access the notebook. |

#### Teams

```
title=teams.name
content=teams.contents
created=teams.created
last_modified=teams.last_modified
url=teams.web_url
role=teams.roles
```

| Key | Value |
| --- | --- |
| teams.name | The name of the team/channel/message. |
| teams.contents | The text contents of the message |
| teams.created | The time at which the message was created. |
| teams.last_modified | The last time the message was modified. |
| teams.web_url | A link for opening the message in Teams. |
| teams.roles | A users/groups who can access the team. |

#### SharePoint Sites

```
title=site.name
content=site.content
mimetype=site.mimetype
created=site.created
last_modified=site.last_modified
url=site.web_url
role=site.roles
```

| Key | Value |
| --- | --- |
| site.name | The name of the site or document. |
| site.content | Rich content including site information and document library metadata for enhanced search. |
| site.contents | The text contents of the document |
| site.mimetype | The MIME type of the document. |
| site.created | The time at which the document was created. |
| site.last_modified | The last time the document was modified. |
| site.web_url | A link for opening the document in a browser. |
| site.roles | A users/groups who can access the document. |

**Note**: The `site.content` field now includes comprehensive site information (name, description, URL) combined with document library metadata to provide richer search content for SharePoint sites.

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
| item.fields | All fields and values from the SharePoint list item |
| item.created | The time at which the list item was created. |
| item.modified | The last time the list item was modified. |
| item.url | A link for opening the list item in SharePoint. |
| item.roles | A users/groups who can access the list item. |

**Note**: The plugin automatically expands SharePoint list item fields to ensure content extraction. If fields are not initially available, it performs an individual API call with `$expand=fields` to retrieve the complete field data.

### Configuration Parameters

#### Common Parameters

| Parameter | Description | Default |
| --- | --- | --- |
| tenant | Azure AD tenant ID | Required |
| client_id | App registration client ID | Required |
| client_secret | App registration client secret | Required |
| number_of_threads | Number of crawling threads | 1 |
| ignore_error | Continue crawling on errors | false |
| include_pattern | Include files/items matching pattern | - |
| exclude_pattern | Exclude files/items matching pattern | - |

#### SharePoint Site Parameters

| Parameter | Description | Default |
| --- | --- | --- |
| site_id | Specific SharePoint site ID to crawl | All sites |
| exclude_site_id | Site IDs to exclude (see format below) | - |
| ignore_system_libraries | Skip system document libraries | true |
| max_content_length | Maximum content length to extract | 10485760 |

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

#### SharePoint List Parameters

| Parameter | Description | Default |
| --- | --- | --- |
| site_id | SharePoint site ID containing lists | Required |
| list_id | Specific list ID to crawl | All lists |
| exclude_list_id | Comma-separated list IDs to exclude | - |
| list_template_filter | Filter by list template types | - |
| ignore_system_lists | Skip system lists | true |
| include_attachments | Include list item attachments | false |

**Recent Improvements**: SharePoint List crawling now includes enhanced statistical tracking, improved error handling with configurable failure recording, comprehensive URL filtering support, and robust permission processing to ensure secure and efficient list item indexing.

## Azure App Registration

To use this plugin, you need to create an Azure App registration with the following permissions:

### Required API Permissions

- **Microsoft Graph**:
  - Files.Read.All
  - Sites.Read.All
  - Notes.Read.All
  - Chat.Read.All
  - ChannelMessage.Read.All
  - Team.ReadBasic.All
  - User.Read.All
  - Group.Read.All

### Authentication

The plugin uses Client Credentials flow with client secret authentication.

## Development

### Build

```bash
mvn clean package
```

### Code Formatting

```bash
mvn formatter:format
```

### Test

```bash
mvn test
```

## License

[ASL 2.0](https://github.com/codelibs/fess-ds-microsoft365/blob/master/LICENSE)
