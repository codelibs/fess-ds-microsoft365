# Upgrading

Notes for moving an existing installation to a newer release: what to re-crawl or re-check after the upgrade. Every section below applies when upgrading from 15.8.x or earlier to 15.9.0 or later; the current behaviour is described in [README.md](README.md).

## SharePoint list items: Links titles, column-only content and rich text

**Note**: Before this release a Links list item was indexed with no title, because a Links list hides its `Title` field, and it
bypassed `include_pattern` and `exclude_pattern`. Re-crawl to fill in those titles; with either pattern set, Links list items are
now filtered by that title as well.

**Note**: Before this release an item with none of `Body`, `Description`, `Comments` or `Notes` was indexed with no content, which
left every survey response empty. Re-crawl to fill it in. Each list whose items are indexed now costs one more request,
`GET /sites/{site-id}/lists/{list-id}/columns`, under the same permission as its items.

**Note**: Before this release a rich text field, such as an Announcements list's `Body`, was indexed as raw HTML, tags and
style attributes included. Re-crawl to replace it with the text.

## SharePoint pages: home pages are now typed page

**Note**: Before this release every page was typed `news` or `article`, so `page_type_filter=page` crawled
nothing and `article` also took site home pages. Home pages are now `page`, in both `page_type_filter` and
`page.type`.

## OneDrive: drive-item URL encoding fix

This affects only items whose Graph `webUrl` is a `/_layouts/` viewer URL, which `getUrl()` rewrites
into a path-based URL indexed as `file.url`. Two defects in that rewrite are fixed:

- **Parent folder segments were percent-encoded twice.** Graph documents `parentReference.path` as
  a percent-encoded path (for example `/drive/root:/Documents/my%20file.docx`), but each segment was
  encoded again, so `My%20Folder` became `My%2520Folder` and a non-ASCII folder name became an
  unusable `%25E8%25B3%2587...`. Every file inside a folder whose name contains a space or a
  non-ASCII character got a link that does not resolve. The item's own name was always encoded
  exactly once and was never affected, which is why the problem only showed up below a folder.
- **`drive_id` crawls spliced in the drive's display name unencoded.** A display name containing a
  space produced a URL with a literal space in it, and a drive whose display name differs from its
  URL segment - a localized default library, or one renamed after creation - produced a link to a
  path that does not exist. The drive's own `webUrl` is now used, falling back to the encoded
  display name only when Graph returns no `webUrl`.

Existing documents keep the old URL until OneDriveDataStore is crawled again. Because a Fess
document ID is derived from the URL, a re-crawl indexes the corrected URL as a **new** document
rather than updating the old one, and the stale document is not removed by the crawler's own
cleanup while it still carries an `expires` value (`day.for.cleanup`, 3 days by default). Delete the
data store's existing documents before re-crawling if you do not want both links in the results in
the meantime.

`include_pattern` / `exclude_pattern` match `file.url`, so a pattern written against a
double-encoded path no longer matches. Patterns written against a site or library path are
unaffected.

## SharePoint document libraries: library URL fix

`doclib.url` is the key the default script maps to `url`, so it is the link every search result
points at. It used to be composed from the library's **display name**:
`{siteUrl}/{encodedLibraryName}`, with a special case that emitted `{siteUrl}/Shared%20Documents`
for a library named exactly `Documents` or `Shared Documents`. A SharePoint library's URL segment
is fixed when the library is created, while `Drive.name` is a read-write display name, so the two
diverge in two common cases, and the composed link then resolved to a path that does not exist:

- **The default library of a non-English site.** Graph returns its display name translated, so the
  English-literal special case never matched and the URL was composed from the translated name,
  even though the library still lives at `/Shared Documents`.
- **A library renamed after creation, or created with a name SharePoint could not use verbatim as a
  URL segment.** The display name moved; the URL segment did not.

`doclib.url` is now the drive's Graph `webUrl`, which Graph defines as the URL that displays the
resource in the browser. A URL is composed from the library name only when Graph returns no
`webUrl` at all.

Two things to be aware of on upgrade:

- **Re-crawl to repair the links.** Existing documents keep the composed URL until the document
  library data store is crawled again. Because a Fess document ID is derived from the URL, a
  re-crawl indexes the corrected URL as a **new** document rather than updating the old one, and
  the stale document is not removed by the crawler's own cleanup while it still carries an
  `expires` value (`day.for.cleanup`, 3 days by default). Delete the data store's existing
  documents before re-crawling if you do not want both links in the results in the meantime.
- **Check `include_pattern` / `exclude_pattern`.** They match `doclib.url`, so a pattern written
  against the previously composed URL of a localized or renamed library no longer matches. Patterns
  written against a site path (the documented examples) are unaffected.

## OneNote: delegated authentication and site notebooks on every site

Two things change for `oneNoteDataStore` in this release, and both need a re-crawl to take effect.

- **It can no longer crawl at all with `client_secret` alone.** Microsoft retired app-only tokens
  for the Microsoft Graph OneNote API on 2025-03-31, so an existing OneNote data config has been
  indexing nothing since then while still reporting a successful crawl. Add `username`/`password`
  to that data config and re-crawl; see
  [OneNote requires delegated authentication](README.md#onenote-requires-delegated-authentication) for the
  app-registration changes and for what a delegated token can reach. Until that is done the crawl
  now fails with a message naming the retirement, instead of reporting success with zero documents.
- **Site notebooks now cover every site, not just the root site.** `site_note_crawler=true`
  previously listed notebooks under `GET /sites/root` and nowhere else. It now enumerates every
  site in the tenant, so a re-crawl can add notebooks from team sites that were never indexed
  before, and issues one notebook-listing request per site. Set `site_id` to keep the crawl on a
  single site.

Leave the other data configs on `client_secret`: no other DataStore in this plugin is affected by
the OneNote retirement, and giving them a delegated identity would narrow their crawls to what that
one account can see.

## OneNote: failure URL rows keyed by notebook URL

A notebook that fails to index is recorded in Fess's Failure URL admin screen. That row used to be
keyed by the notebook's **display name**, which is not unique: two notebooks sharing a name
collapsed into one row, and an operator saw one failure where there were two. The row is now keyed
by the notebook's own web URL, falling back to its id when the failure happened before that URL was
read, and to the display name only when the notebook has neither.

Nothing needs re-crawling for this. The visible effects are that failure rows for notebooks are now
followable links rather than bare names, and that a crawl with several same-named notebooks failing
reports the true count instead of one. Any existing saved filter or report keyed on the old
display-name rows will no longer match.

## OneDrive: per-item failure log line

`processDriveItem` has two catch arms, and both used to log `Crawling Access Exception at : {}`.
OneDriveDataStore was the only one of the six data stores whose two failure paths could not be told
apart in the crawler log. The second arm - everything that is not a `CrawlingAccessException` - now
logs `Processing exception at : {}`, the phrasing the other five stores already use.

Both stay at `WARN`, both still record a failure-URL row, and the failure-URL rows are unchanged -
both arms pass the same `item.getWebUrl()`, and a failure-URL row is looked up by URL plus crawl
config (the error name is written onto whatever row that finds), so the two arms have always
shared one row per item and still do. Only the log text differs. **An alert or log filter
grepping for `Crawling Access Exception at` will stop matching non-access failures on OneDrive**;
match `Processing exception at` as well, or drop to matching the shared `at : ` suffix.

## Teams and OneNote: all-teams crawl and the Permissions field

Two changes in this release alter what a Teams crawl produces. Both need a re-crawl, and the
second needs a review before that re-crawl.

- **Crawling all teams indexed zero documents before this release.** This affected configurations
  that leave `team_id` unset (or empty) - the all-teams path, which is the default. A config that
  names a `team_id`, or one that sets `chat_id`, went through `getGroupById` and `getChatMessages`
  instead, never touched the broken code below, and indexed normally; nothing about those two
  changes here. For the all-teams path: `getTeams` enumerated `/teams`,
  fetched each team's backing `Group`, and then checked whether that group's
  `resourceProvisioningOptions` contained `"Team"` - reading the value out of
  `Group#getAdditionalData()`. In Microsoft Graph SDK v6 `resourceProvisioningOptions` is a
  **typed** property on `Group`, so Kiota registers a field deserializer for it and routes it to
  the setter; it never reaches the additional-data map, and the two are disjoint. The check
  therefore rejected **every** team, the consumer was never called, and the crawl reported success
  while indexing nothing. The only trace was one `DEBUG` line per skipped team. The typed accessor
  is read now, and the check rejects a group only when the list is present and lacks `"Team"`.
  **On an all-teams config, expect the indexed document count to jump from zero on the first crawl
  after upgrading** - that is the fix working, not a runaway crawl, so do not kill the job for it.
  Budget for the full volume of every team's channel messages. A `team_id` or `chat_id` config
  will see no change in volume from this fix.

  A `team_id` set to the **empty string** (`team_id=`) counts as an all-teams config and gets the
  same jump, from a different cause: 15.8.0 read `} else if (teamId == null) {`, so a present-but-
  empty value matched neither the specific-team branch nor the all-teams one and crawled nothing
  while reporting success. Both spellings now mean "all teams". If `team_id=` was being used as a
  way to keep a data config in place without crawling, disable the config instead.

  Note also that **archived teams are crawled**. The old check was named `isActiveTeam`, which
  implied it excluded them; it never looked at `Team#getIsArchived`, and neither does the current
  code. Use `exclude_team_ids` to skip archived teams you do not want indexed.

- **The data config's Permissions field now reaches Teams message ACLs.** At 15.8.0 **two** of the
  six data stores never folded `defaultDataMap`'s role entry into a document's role list:
  TeamsDataStore and OneNoteDataStore. A Teams or OneNote data config's Permissions field was
  silently discarded for every message or notebook it indexed. Both apply it now, in the same order
  the other four use: the store's own roles, then `default_permissions`, then the data config's
  Permissions field.

  This **widens** who can retrieve Teams messages and OneNote notebooks - back to what the config
  asked for, but wider than what has actually been in the index. **Audit the Permissions field on
  every Teams and OneNote data config before re-crawling.** A config whose Permissions field was
  cloned from another data store's config has been carrying that other store's audience
  harmlessly; after this release it grants it. The change is purely additive - no document loses a
  role - and already-indexed Teams and OneNote documents keep their old, narrower ACL until they
  are re-crawled.

  For OneNote this lands on **every** notebook type, user notebooks included - and unlike site and
  group notebooks, user notebooks were already being indexed under 15.8.0, so this is a change to
  the audience of documents that are already in the index rather than an ACL on documents that are
  new.

  It is inert for one configuration: if you removed the `role=message.roles` line from the script,
  the roles Teams computes never reach the document, and the data config's Permissions field
  already reached it directly through the default data map. Nothing changes there.

## SharePoint pages: standard web-part text

Standard web parts (everything except plain text web parts - Quick Links, Hero, Events, News,
and so on) previously contributed **nothing** to `page.content`: the extractor received a typed
`WebPartData` object it could not read, so it appended no characters. It now extracts
SharePoint's own indexable projection of the web part (`serverProcessedContent.searchablePlainTexts`
and `htmlStrings`).

It also walks the web part's `additionalData` map - a forward-compatibility read for any Graph
field the SDK's typed `WebPartData` model does not (yet) declare, not a source of text today. On
the pinned Graph SDK version, `WebPartData` already models every documented field except
`audiences`, so as of this SDK version `additionalData` contributes no text - and no noise - to
`page.content`.

Note what the walk can and cannot pick up. It descends into `java.util.Map` and `java.util.List`
values and reads `String` values; anything else is skipped. An undeclared Graph field arrives as a
Kiota untyped node, and only a **scalar string** one is read - an undeclared object or array field
deserializes to `UntypedObject`/`UntypedArray`, which are none of those three types and are
dropped before the GUID filter is ever consulted. `audiences` is an array, so that is the reason it
contributes nothing, not the GUID filter. The read is kept because it costs nothing and covers the
scalar case, not because it is a general forward-compatibility guarantee.

No field is renamed or removed and `page.content` never shrinks, so existing scripts and index
mappings keep working unchanged. But **existing indexes will not contain the new text until the
pages are re-crawled**, and once re-crawled those documents have longer content and therefore
different relevance scores. Re-crawl the SharePoint Pages data store after upgrading if you rely
on web-part text being searchable.

The web part's free-form `properties` object is still not read: Microsoft Graph exposes it as an
untyped node with no published schema.

## Graph client: access_timeout now takes effect

`access_timeout` is not a new parameter: the constant has been declared since the plugin's first
release and was never read anywhere, so setting it had no effect. This release is the first time
it does anything - it now sets OkHttp's call timeout - so if you already have `access_timeout`
configured, upgrading changes its behavior from a no-op to an active timeout.

## OneNote: include_pattern and exclude_pattern now filter notebooks

Note that `oneNoteDataStore` ignored both parameters in earlier releases - a configuration that
set them expecting them to be a no-op will start filtering notebooks after upgrading.

## SharePoint lists, SharePoint document libraries and OneDrive: crawl filter fixes

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
- **`ignore_system_libraries`** never excluded Style Library or Form Templates
  (`FormServerTemplates`). The system-library check compared the drive's URL against patterns
  ending in a slash, such as `/style%20library/`, but a drive's Graph `webUrl` ends with the
  library's own URL segment (`.../Style%20Library`), so of the libraries it was meant to exclude
  only those under `/_catalogs/` could match. It also matched `/forms/` anywhere in the URL, so a
  site whose path contains a `Forms` segment (e.g. a site collection named "Forms") had every one
  of its libraries treated as a system library. The check now compares the library's own URL
  segment; `Forms` is no longer part of it, since it is the folder inside every library that holds
  its view pages rather than a library. Graph does not list a drive that carries the `system`
  facet unless the request selects it. The plugin now selects it only with
  `ignore_system_libraries=false`, so that setting is what makes such a drive appear; at the
  default `true` it stays hidden, as before. Whether a given library (for example Style Library)
  carries the facet is up to SharePoint: if a tenant's site does not return it with
  `GET /sites/{site-id}/drives?$select=id,name,webUrl,driveType,system`, there is nothing for the
  plugin to crawl.
  - **SharePointDocLibDataStore:** with the default `true`, Style Library and Form Templates are no
    longer indexed, and the libraries of a site whose path contains `Forms` now are. Re-crawl to
    apply both, or set `ignore_system_libraries=false` first if you want to keep indexing the
    system libraries.
  - **OneDriveDataStore:** when it crawls all SharePoint sites' document libraries (Crawling Mode
    1, which runs whenever `shared_documents_drive_crawler=true`, the default), no system-library
    check was made at all, so the default `true` changed nothing there. Mode 1 enforces it now,
    still by default. Because Mode 1 indexes the files inside those libraries, not just library
    metadata the way SharePointDocLibDataStore does, the drop in indexed items after a re-crawl can
    be far larger here. Re-crawl OneDriveDataStore to remove them from the index, or set
    `ignore_system_libraries=false` first if you want to keep indexing them.

  **The parameter still does nothing in Modes 2 and 3.** A user's personal drive
  (`user_drive_crawler`) and a group's drive (`group_drive_crawler`) are crawled whatever
  `ignore_system_libraries` says; those two paths evaluate `isSystemLibrary` only to fill in a
  `debug` log line, exactly as they did before this release. Nothing changes there on upgrade.
- **`ignore_system_lists`** (SharePointListDataStore) did not exclude hidden lists. The crawl
  asks Graph for system-managed lists as well (`system` is in its `$select`), but the check only
  matched URL patterns and never read Graph's `system` facet or `list.hidden`. A hidden list whose
  URL is an ordinary `/Lists/...` path and whose template is `genericList` - such as
  `TaxonomyHiddenList`, the site's cache of managed metadata terms - was therefore indexed like a
  list a user created. With the default `true`, a list carrying either flag is now skipped, whether
  it is found by enumerating a site's lists or named by `list_id`. Re-crawl to remove its items, or
  set `ignore_system_lists=false` first if you want to keep indexing them.
- **`include_pattern` / `exclude_pattern`** (SharePointDocLibDataStore) were declared as constants
  but never read anywhere, so configuring either one had no effect at all. They now filter document
  libraries by their browser URL (`doclib.url`) - see
  [SharePoint Document Library Parameters](README.md#sharepoint-document-library-parameters) below for what
  that means in practice. If you had already configured either parameter for this DataStore
  expecting it to work, re-crawl to have it apply for the first time.
- **`shared_documents_drive_crawler`** (OneDriveDataStore) also crawled every user's personal
  OneDrive. `GET /sites` returns personal sites (`https://{tenant}-my.sharepoint.com/personal/...`)
  alongside SharePoint sites, and Mode 1 crawled every drive they had, so a configuration with
  `user_drive_crawler=false` still indexed personal files. Mode 1 now crawls only drives whose
  `driveType` is `documentLibrary`, the same check SharePointDocLibDataStore already made; a
  personal site's drive is `business` and is skipped. Personal OneDrive is crawled only by Mode 2
  (`user_drive_crawler`). Re-crawl to remove the personal files from the index.

## SharePoint document libraries: system libraries and pattern filters now apply

> **Behavior changes in this release:**
> - `ignore_system_libraries` (default `true`) did not exclude Style Library or Form Templates here
>   before this release, and treated every library of a site whose path contains a `Forms` segment
>   as a system library. With the default, Style Library and Form Templates are now skipped and
>   such a site's libraries are crawled - see
>   [SharePoint lists, SharePoint document libraries and OneDrive: crawl filter fixes](#sharepoint-lists-sharepoint-document-libraries-and-onedrive-crawl-filter-fixes)
>   above for what changes on upgrade.
> - `include_pattern` / `exclude_pattern` were declared but never referenced anywhere in this
>   DataStore, so configuring them previously did nothing. They are wired to a Fess `UrlFilter` now,
>   matched against the library's browser URL (`doclib.url`, generated by
>   `generateDocumentLibraryUrl(site, drive)`) - **not** the library's display name. This is the same
>   `UrlFilter` mechanism OneDriveDataStore uses.

## SharePoint lists, pages and OneNote site notebooks: Sites.FullControl.All no longer needed

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
to the wrong Graph path and 404'd - not because of a permissions problem. See [OneDrive, SharePoint document libraries and OneNote: ACL correction](#onedrive-sharepoint-document-libraries-and-onenote-acl-correction) below for how that ACL is built.

## OneDrive, SharePoint document libraries and OneNote: ACL correction

This fix changes which ACL gets indexed, not just how failures in retrieving it are handled, so
documents indexed by an older version keep their old ACL until they are re-crawled:

- For OneDrive and SharePoint document libraries, a user or
  group named in an ACL entry used to resolve only to an internal object ID, which matches no
  Fess role; the UPN (for users) and group name (for groups) roles that should have been added
  alongside it were silently dropped. They are added now, so re-crawled documents typically gain
  roles rather than lose them. The group name added is the group's `mail` attribute if it has
  one, otherwise `mailNickname`, otherwise `displayName` - so it is usually an email address, not
  the group's display name. If a Fess role mapping was built around the group's display name,
  check it against whichever of these attributes the group actually has. This applies to these
  two DataStores only: they are the only ones that read a Graph permissions endpoint. SharePoint
  lists and SharePoint pages resolve no user or group at all - their ACL is `default_permissions`
  plus the data config's Permissions field, and it does not change on upgrade (see
  [What ACL Each DataStore Can Produce](README.md#what-acl-each-datastore-can-produce) above).
- OneNote site and group notebooks (`site_note_crawler` and `group_note_crawler`, both enabled
  by default) were not indexed by any earlier release at all. A client-side bug sent every
  notebook, section, page, and page-content request for them to the wrong Graph path; Graph
  returned `404` for all of it, and that was logged only at `debug` level, so the crawl reported
  success while indexing zero notebooks. This is not an ACL correction like the ones above - it
  is the first time these notebooks are indexed at all. Site notebooks previously carried an
  unconditionally empty ACL; they now carry `default_permissions` when it is configured (see
  [What ACL Each DataStore Can Produce](README.md#what-acl-each-datastore-can-produce) above) -
  `permission_failure_policy` still has no effect on OneNoteDataStore, because it never calls a
  Graph permission-fetch endpoint for any notebook type. A `404` that persists after re-crawling
  for a site's or group's notebooks is
  now logged at `WARN` and means the site or group genuinely has no notebooks, not that the
  crawler is asking the wrong Graph path; a user's `404` stays at `debug` (see "404 Visibility"
  below).

Re-crawl the OneDrive and SharePoint document library crawlers after upgrading to pick up the
corrected ACLs, and re-crawl OneNoteDataStore (with `site_note_crawler` and/or
`group_note_crawler` enabled) to index the site and group notebooks for the first time. There is
no ACL correction to pick up for SharePoint lists or SharePoint pages.
