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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.codelibs.fess.ds.ms365.SensitivityLabelPolicy.Decision;
import org.codelibs.fess.ds.ms365.SensitivityLabelPolicy.Label;
import org.codelibs.fess.ds.ms365.SensitivityLabelPolicy.Rule;
import org.junit.jupiter.api.Test;

/**
 * Covers the parsing and evaluation of {@code sensitivity_label_policy}. The policy decides
 * whether a labeled file is indexed, whether its content is downloaded and which roles may see
 * it, so a rule that silently matches the wrong label - or none - either leaks a confidential
 * document or drops an ordinary one.
 */
public class SensitivityLabelPolicyTest {

    /** Stands in for {@code PermissionHelper#encode} without a container. */
    private static final Function<String, String> ENCODER = s -> s.replace("{group}", "2").replace("{user}", "1").replace("{role}", "R");

    private static final String ID_CONFIDENTIAL = "0e7d7d2b-1c3e-4f5a-9b8c-1234567890ab";
    private static final String ID_SECRET = "a1b2c3d4-e5f6-4a5b-8c9d-0123456789ef";
    private static final String ID_OTHER = "ffffffff-0000-4000-8000-000000000001";

    private static SensitivityLabelPolicy parse(final String value) {
        return SensitivityLabelPolicy.parse(value, ENCODER);
    }

    private static Label resolved(final String id, final Boolean hasProtection, final String... names) {
        return new Label(id, Arrays.asList(names), hasProtection, true);
    }

    // ===== parse: empty input =====

    @Test
    public void test_parse_blankIsEmpty() {
        assertTrue(parse(null).isEmpty());
        assertTrue(parse("").isEmpty());
        assertTrue(parse("  \n \r\n\t").isEmpty());
    }

    @Test
    public void test_parse_commentsOnlyIsEmpty() {
        assertTrue(parse("# a comment\n\n   # an indented comment\r\n").isEmpty());
    }

    @Test
    public void test_parse_ignoresCommentsAndBlankLines() {
        final SensitivityLabelPolicy policy = parse("# exclude confidential files\n\n  Confidential = skip  \r\n   # trailing comment\n");
        assertFalse(policy.isEmpty());
        assertEquals(new Rule(true, false, null), policy.findRule(resolved(ID_CONFIDENTIAL, false, "Confidential")));
    }

    @Test
    public void test_isEmpty_falseForEachRuleKind() {
        assertFalse(parse(ID_CONFIDENTIAL + "=skip").isEmpty());
        assertFalse(parse("Confidential=skip").isEmpty());
        assertFalse(parse("@protected=index").isEmpty());
        assertFalse(parse("*=no_content").isEmpty());
    }

    // ===== findRule: matching =====

    @Test
    public void test_findRule_matchesIdCaseInsensitively() {
        final SensitivityLabelPolicy policy = parse(ID_CONFIDENTIAL.toUpperCase() + "=skip");
        assertEquals(new Rule(true, false, null), policy.findRule(Label.unresolved(ID_CONFIDENTIAL)));
        assertEquals(new Rule(true, false, null), policy.findRule(resolved(ID_CONFIDENTIAL.toUpperCase(), false, "Confidential")));
        assertNull(policy.findRule(Label.unresolved(ID_OTHER)));
    }

    @Test
    public void test_findRule_matchesEitherNameCaseInsensitively() {
        final SensitivityLabelPolicy policy = parse("highly confidential=skip");
        // the display name
        assertEquals(new Rule(true, false, null), policy.findRule(resolved(ID_CONFIDENTIAL, false, "Highly Confidential", "HC")));
        // the name, when the display name does not match
        assertEquals(new Rule(true, false, null), policy.findRule(resolved(ID_SECRET, false, "Streng vertraulich", "HIGHLY CONFIDENTIAL")));
        assertNull(policy.findRule(resolved(ID_OTHER, false, "General", "Public")));
    }

    @Test
    public void test_findRule_ignoresBlankAndNullNames() {
        final SensitivityLabelPolicy policy = parse("Confidential=skip");
        assertEquals(new Rule(true, false, null), policy.findRule(resolved(ID_CONFIDENTIAL, false, null, "  ", "Confidential")));
    }

    @Test
    public void test_findRule_doesNotMatchNamesOfUnresolvedLabel() {
        // A label whose definition was not read has no trustworthy names.
        final SensitivityLabelPolicy policy = parse("Confidential=skip");
        assertNull(policy.findRule(new Label(ID_CONFIDENTIAL, List.of("Confidential"), null, false)));
    }

    @Test
    public void test_findRule_precedenceIdThenNameThenProtectedThenAny() {
        final SensitivityLabelPolicy policy =
                parse(ID_CONFIDENTIAL + "=restrict:{group}a\nConfidential=skip\n@protected=index\n*=no_content");

        // id beats name and @protected
        assertEquals(new Rule(false, false, Set.of("2a")), policy.findRule(resolved(ID_CONFIDENTIAL, true, "Confidential")));
        // name beats @protected
        assertEquals(new Rule(true, false, null), policy.findRule(resolved(ID_SECRET, true, "Confidential")));
        // @protected beats *
        assertEquals(new Rule(false, false, null), policy.findRule(resolved(ID_SECRET, true, "Secret")));
        // * for everything else
        assertEquals(new Rule(false, true, null), policy.findRule(resolved(ID_SECRET, false, "Secret")));
        assertEquals(new Rule(false, true, null), policy.findRule(resolved(ID_SECRET, null, "Secret")));
    }

    @Test
    public void test_findRule_protectedKeyIsCaseInsensitive() {
        final SensitivityLabelPolicy policy = parse("@Protected=skip");
        assertEquals(new Rule(true, false, null), policy.findRule(resolved(ID_SECRET, true, "Secret")));
    }

    // ===== findRule: implicit rule for encrypted labels =====

    @Test
    public void test_findRule_defaultProtectedRuleWhenNoProtectedRuleConfigured() {
        final Rule expected = new Rule(false, true, null);
        assertEquals(expected, SensitivityLabelPolicy.DEFAULT_PROTECTED_RULE);

        assertEquals(expected, parse("").findRule(resolved(ID_SECRET, true, "Secret")));
        assertEquals(expected, parse("Confidential=skip").findRule(resolved(ID_SECRET, true, "Secret")));
        // with *=index, the implicit rule is * with no_content forced on
        assertEquals(expected, parse("*=index").findRule(resolved(ID_SECRET, true, "Secret")));
        // an unencrypted label does not get it
        assertNull(parse("").findRule(resolved(ID_SECRET, false, "Secret")));
        assertNull(parse("").findRule(resolved(ID_SECRET, null, "Secret")));
    }

    @Test
    public void test_findRule_implicitProtectedRuleKeepsAnyRuleSkip() {
        // An operator who wrote *=skip means every labeled file, encrypted or not.
        final SensitivityLabelPolicy policy = parse("*=skip");
        final Rule rule = policy.findRule(resolved(ID_SECRET, true, "Secret"));
        assertTrue(rule.skip());
        assertTrue(rule.noContent());
        assertNull(rule.restrictRoles());
        assertTrue(policy.decide(List.of(resolved(ID_SECRET, true, "Secret"))).skip());
    }

    @Test
    public void test_findRule_implicitProtectedRuleKeepsAnyRuleRestriction() {
        final SensitivityLabelPolicy policy = parse("*=restrict:{group}a");
        assertEquals(new Rule(false, true, Set.of("2a")), policy.findRule(resolved(ID_SECRET, true, "Secret")));
        assertEquals(new Decision(false, true, Set.of("2a")), policy.decide(List.of(resolved(ID_SECRET, true, "Secret"))));
        // an unencrypted label gets * unchanged
        assertEquals(new Rule(false, false, Set.of("2a")), policy.findRule(resolved(ID_SECRET, false, "Secret")));
    }

    @Test
    public void test_findRule_explicitProtectedRuleTakesPrecedenceOverAny() {
        final SensitivityLabelPolicy policy = parse("*=skip\n@protected=restrict:{group}a");
        assertEquals(new Rule(false, false, Set.of("2a")), policy.findRule(resolved(ID_SECRET, true, "Secret")));
        assertEquals(new Rule(true, false, null), policy.findRule(resolved(ID_SECRET, false, "Secret")));
    }

    @Test
    public void test_findRule_nameRuleTakesPrecedenceOverImplicitProtectedRule() {
        final SensitivityLabelPolicy policy = parse("Secret=index\n*=skip");
        assertEquals(new Rule(false, false, null), policy.findRule(resolved(ID_SECRET, true, "Secret")));
    }

    @Test
    public void test_findRule_explicitProtectedRuleOverridesDefault() {
        final SensitivityLabelPolicy policy = parse("@protected=index");
        assertEquals(new Rule(false, false, null), policy.findRule(resolved(ID_SECRET, true, "Secret")));
        assertFalse(policy.decide(List.of(resolved(ID_SECRET, true, "Secret"))).noContent());
    }

    @Test
    public void test_findRule_unresolvedLabelGetsAnyRuleButNotDefaultProtectedRule() {
        // hasProtection is ignored for a label whose definition was not read
        final Label unresolvedEncrypted = new Label(ID_SECRET, Collections.emptyList(), Boolean.TRUE, false);
        assertNull(parse("").findRule(unresolvedEncrypted));
        assertNull(parse("").findRule(Label.unresolved(ID_SECRET)));
        assertEquals(new Rule(true, false, null), parse("*=skip").findRule(Label.unresolved(ID_SECRET)));
        assertEquals(new Rule(true, false, null), parse("*=skip").findRule(unresolvedEncrypted));
    }

    // ===== findRule: sublabels =====

    private static Label sublabel(final String id, final Boolean hasProtection, final List<String> names, final String parentId,
            final String... parentNames) {
        return new Label(id, names, hasProtection, true, parentId, Arrays.asList(parentNames));
    }

    @Test
    public void test_findRule_matchesParentId() {
        final SensitivityLabelPolicy policy = parse(ID_CONFIDENTIAL.toUpperCase() + "=skip");
        assertEquals(new Rule(true, false, null),
                policy.findRule(sublabel(ID_SECRET, false, List.of("All Employees"), ID_CONFIDENTIAL, "Confidential")));
    }

    @Test
    public void test_findRule_matchesEitherParentName() {
        final SensitivityLabelPolicy policy = parse("confidential=no_content");
        assertEquals(new Rule(false, true, null),
                policy.findRule(sublabel(ID_SECRET, false, List.of("All Employees"), ID_CONFIDENTIAL, "Vertraulich", "CONFIDENTIAL")));
        assertEquals(new Rule(false, true, null),
                policy.findRule(sublabel(ID_SECRET, false, List.of("All Employees"), null, null, " ", "Confidential")));
    }

    @Test
    public void test_findRule_ownRuleBeatsParentRule() {
        final Label label = sublabel(ID_SECRET, false, List.of("All Employees"), ID_CONFIDENTIAL, "Confidential");
        // own id beats parent id and parent name
        assertEquals(new Rule(false, false, null),
                parse(ID_SECRET + "=index\n" + ID_CONFIDENTIAL + "=skip\nConfidential=skip").findRule(label));
        // own name beats parent id
        assertEquals(new Rule(false, true, null), parse("All Employees=no_content\n" + ID_CONFIDENTIAL + "=skip").findRule(label));
        // parent id beats parent name
        assertEquals(new Rule(true, false, null), parse(ID_CONFIDENTIAL + "=skip\nConfidential=index").findRule(label));
    }

    @Test
    public void test_findRule_parentRuleBeatsProtectedAndAny() {
        final Label encrypted = sublabel(ID_SECRET, true, List.of("All Employees"), ID_CONFIDENTIAL, "Confidential");
        assertEquals(new Rule(false, false, null), parse("Confidential=index\n@protected=skip\n*=skip").findRule(encrypted));
        assertEquals(new Rule(false, false, null), parse(ID_CONFIDENTIAL + "=index").findRule(encrypted),
                "the parent rule also beats the implicit rule for encrypted labels");
        final Label plain = sublabel(ID_SECRET, false, List.of("All Employees"), ID_CONFIDENTIAL, "Confidential");
        assertEquals(new Rule(false, false, Set.of("2a")), parse("Confidential=restrict:{group}a\n*=skip").findRule(plain));
    }

    @Test
    public void test_findRule_sublabelWithoutParentRuleFallsThrough() {
        final Label encrypted = sublabel(ID_SECRET, true, List.of("All Employees"), ID_CONFIDENTIAL, "Confidential");
        assertEquals(new Rule(true, false, null), parse("Public=index\n@protected=skip").findRule(encrypted));
        assertEquals(new Rule(false, true, null), parse("Public=index").findRule(encrypted));
        final Label plain = sublabel(ID_SECRET, false, List.of("All Employees"), ID_CONFIDENTIAL, "Confidential");
        assertEquals(new Rule(true, false, null), parse("Public=index\n*=skip").findRule(plain));
    }

    @Test
    public void test_findRule_parentIgnoredForUnresolvedLabel() {
        final Label label = new Label(ID_SECRET, List.of(), null, false, ID_CONFIDENTIAL, List.of("Confidential"));
        assertNull(parse(ID_CONFIDENTIAL + "=skip\nConfidential=skip").findRule(label));
    }

    // ===== canEvaluate =====

    @Test
    public void test_canEvaluate_resolvedLabelAlways() {
        assertTrue(parse("Confidential=skip\n@protected=skip").canEvaluate(resolved(ID_OTHER, false, "General")));
    }

    @Test
    public void test_canEvaluate_unresolvedWithoutDefinitionDependentRules() {
        final Label label = Label.unresolved(ID_OTHER);
        assertTrue(parse("").canEvaluate(label));
        assertTrue(parse("*=skip").canEvaluate(label));
    }

    @Test
    public void test_canEvaluate_unresolvedNotMatchedWithIdRulesOnly() {
        // An ID rule can match a sublabel through its parent, so it needs the definition too.
        final SensitivityLabelPolicy policy = parse(ID_CONFIDENTIAL + "=skip\n" + ID_SECRET.toUpperCase() + "=no_content");
        assertFalse(policy.canEvaluate(Label.unresolved(ID_OTHER)));
        assertTrue(policy.canEvaluate(Label.unresolved(ID_SECRET)));
    }

    @Test
    public void test_canEvaluate_unresolvedMatchedById() {
        final SensitivityLabelPolicy policy = parse(ID_CONFIDENTIAL.toUpperCase() + "=skip\nConfidential=index\n@protected=index");
        assertTrue(policy.canEvaluate(Label.unresolved(ID_CONFIDENTIAL)));
    }

    @Test
    public void test_canEvaluate_unresolvedNotMatchedWithNameRule() {
        final SensitivityLabelPolicy policy = parse(ID_CONFIDENTIAL + "=skip\nSecret=skip");
        assertFalse(policy.canEvaluate(Label.unresolved(ID_OTHER)));
        assertFalse(policy.canEvaluate(Label.unresolved(null)));
    }

    @Test
    public void test_canEvaluate_unresolvedNotMatchedWithExplicitProtectedRule() {
        assertFalse(parse("@protected=index").canEvaluate(Label.unresolved(ID_OTHER)));
        assertFalse(parse(ID_CONFIDENTIAL + "=skip\n@protected=skip\n*=index").canEvaluate(Label.unresolved(ID_OTHER)));
    }

    // ===== parse: actions =====

    @Test
    public void test_parse_multipleActions() {
        final Rule rule =
                parse("Confidential=no_content;restrict:{group}a,{group}b").findRule(resolved(ID_CONFIDENTIAL, false, "Confidential"));
        assertFalse(rule.skip());
        assertTrue(rule.noContent());
        assertEquals(List.of("2a", "2b"), new ArrayList<>(rule.restrictRoles()), "encoded and in configured order");
    }

    @Test
    public void test_parse_actionsTolerateWhitespaceCaseAndEmptySegments() {
        final Rule rule = parse("Confidential = NO_CONTENT ; Restrict : {group}a , , {user}b ;")
                .findRule(resolved(ID_CONFIDENTIAL, false, "Confidential"));
        assertEquals(new Rule(false, true, Set.of("2a", "1b")), rule);
        assertEquals(List.of("2a", "1b"), new ArrayList<>(rule.restrictRoles()));
    }

    @Test
    public void test_parse_indexCombinedWithSkip() {
        assertEquals(new Rule(true, false, null),
                parse("Confidential=index;skip").findRule(resolved(ID_CONFIDENTIAL, false, "Confidential")));
    }

    @Test
    public void test_parse_restrictDropsRolesEncodedAsBlank() {
        final Function<String, String> encoder = s -> s.startsWith("{bogus}") ? null : ENCODER.apply(s);
        final Rule rule = SensitivityLabelPolicy.parse("Confidential=restrict:{bogus}x,{role}admin", encoder)
                .findRule(resolved(ID_CONFIDENTIAL, false, "Confidential"));
        assertEquals(Set.of("Radmin"), rule.restrictRoles());
    }

    @Test
    public void test_parse_restrictRolesAreUnmodifiable() {
        final Rule rule = parse("Confidential=restrict:{group}a").findRule(resolved(ID_CONFIDENTIAL, false, "Confidential"));
        assertThrows(UnsupportedOperationException.class, () -> rule.restrictRoles().add("2z"));
    }

    // ===== parse: errors =====

    @Test
    public void test_parse_missingEquals() {
        assertThrows(IllegalArgumentException.class, () -> parse("Confidential"));
        assertThrows(IllegalArgumentException.class, () -> parse("Public=index\nConfidential skip"));
    }

    @Test
    public void test_parse_missingLabel() {
        assertThrows(IllegalArgumentException.class, () -> parse("=skip"));
        assertThrows(IllegalArgumentException.class, () -> parse("   = skip"));
    }

    @Test
    public void test_parse_emptyAction() {
        assertThrows(IllegalArgumentException.class, () -> parse("Confidential="));
        assertThrows(IllegalArgumentException.class, () -> parse("Confidential=  "));
        assertThrows(IllegalArgumentException.class, () -> parse("Confidential= ; ;"));
    }

    @Test
    public void test_parse_unknownAction() {
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> parse("Confidential=delete"));
        assertTrue(e.getMessage().contains("delete"), e.getMessage());
        assertThrows(IllegalArgumentException.class, () -> parse("Confidential=skip;nocontent"));
    }

    @Test
    public void test_parse_argumentToActionWithoutArgument() {
        assertThrows(IllegalArgumentException.class, () -> parse("Confidential=skip:x"));
        assertThrows(IllegalArgumentException.class, () -> parse("Confidential=no_content:x"));
        assertThrows(IllegalArgumentException.class, () -> parse("Confidential=index:"));
    }

    @Test
    public void test_parse_restrictWithoutRoles() {
        assertThrows(IllegalArgumentException.class, () -> parse("Confidential=restrict"));
        assertThrows(IllegalArgumentException.class, () -> parse("Confidential=restrict:"));
        assertThrows(IllegalArgumentException.class, () -> parse("Confidential=restrict: , ,"));
        assertThrows(IllegalArgumentException.class, () -> SensitivityLabelPolicy.parse("Confidential=restrict:{group}a", s -> null));
    }

    @Test
    public void test_parse_duplicateRestrict() {
        assertThrows(IllegalArgumentException.class, () -> parse("Confidential=restrict:{group}a;restrict:{group}b"));
    }

    @Test
    public void test_parse_duplicateKeys() {
        assertThrows(IllegalArgumentException.class, () -> parse("Confidential=skip\nconfidential=index"));
        assertThrows(IllegalArgumentException.class, () -> parse(ID_CONFIDENTIAL + "=skip\n" + ID_CONFIDENTIAL.toUpperCase() + "=index"));
        assertThrows(IllegalArgumentException.class, () -> parse("*=skip\n*=index"));
        assertThrows(IllegalArgumentException.class, () -> parse("@protected=skip\n@PROTECTED=index"));
    }

    // ===== decide =====

    @Test
    public void test_decide_noLabelsIsNone() {
        assertEquals(Decision.NONE, parse("*=skip\n@protected=skip").decide(Collections.emptyList()));
    }

    @Test
    public void test_decide_noMatchingRuleIsNone() {
        assertEquals(Decision.NONE, parse("Confidential=skip").decide(List.of(resolved(ID_OTHER, false, "General"))));
    }

    @Test
    public void test_decide_skipIfAnyLabelSkips() {
        final SensitivityLabelPolicy policy = parse("Confidential=skip\nGeneral=index");
        final Decision decision =
                policy.decide(List.of(resolved(ID_OTHER, false, "General"), resolved(ID_CONFIDENTIAL, false, "Confidential")));
        assertTrue(decision.skip());
    }

    @Test
    public void test_decide_noContentIfAnyLabelAsks() {
        final SensitivityLabelPolicy policy = parse("Confidential=no_content\nGeneral=index");
        final Decision decision =
                policy.decide(List.of(resolved(ID_OTHER, false, "General"), resolved(ID_CONFIDENTIAL, false, "Confidential")));
        assertFalse(decision.skip());
        assertTrue(decision.noContent());
        assertNull(decision.allowedRoles());
    }

    @Test
    public void test_decide_restrictListsAreIntersected() {
        final SensitivityLabelPolicy policy =
                parse("Confidential=restrict:{group}a,{group}b,{group}c\nSecret=restrict:{group}d,{group}c,{group}b");
        final Decision decision =
                policy.decide(List.of(resolved(ID_CONFIDENTIAL, false, "Confidential"), resolved(ID_SECRET, false, "Secret")));
        assertEquals(List.of("2b", "2c"), new ArrayList<>(decision.allowedRoles()));
        assertFalse(decision.skip());
        assertFalse(decision.noContent());
        assertThrows(UnsupportedOperationException.class, () -> decision.allowedRoles().add("2z"));
    }

    @Test
    public void test_decide_unrestrictedLabelDoesNotWidenRestriction() {
        final SensitivityLabelPolicy policy = parse("Confidential=restrict:{group}a\nGeneral=index");
        final Decision decision =
                policy.decide(List.of(resolved(ID_OTHER, false, "General"), resolved(ID_CONFIDENTIAL, false, "Confidential")));
        assertEquals(Set.of("2a"), decision.allowedRoles());
    }

    @Test
    public void test_decide_disjointRestrictionsLeaveNoRole() {
        final SensitivityLabelPolicy policy = parse("Confidential=restrict:{group}a\nSecret=restrict:{group}b");
        final Decision decision =
                policy.decide(List.of(resolved(ID_CONFIDENTIAL, false, "Confidential"), resolved(ID_SECRET, false, "Secret")));
        assertTrue(decision.allowedRoles().isEmpty(), "an empty restriction is not the same as no restriction");
        assertEquals(List.of(), decision.applyRoles(List.of("2a", "2b")));
    }

    @Test
    public void test_decide_combinesAllActions() {
        final SensitivityLabelPolicy policy = parse("Confidential=no_content;restrict:{group}a,{group}b\n*=restrict:{group}b");
        final Decision decision =
                policy.decide(List.of(resolved(ID_CONFIDENTIAL, false, "Confidential"), resolved(ID_SECRET, false, "Secret")));
        assertEquals(new Decision(false, true, Set.of("2b")), decision);
    }

    // ===== Decision.applyRoles =====

    @Test
    public void test_applyRoles_noRestrictionReturnsInput() {
        final List<String> roles = List.of("1alice", "2a");
        assertSame(roles, Decision.NONE.applyRoles(roles));
    }

    @Test
    public void test_applyRoles_keepsOriginalOrderAndFilters() {
        final Decision decision = new Decision(false, false, new java.util.LinkedHashSet<>(List.of("2b", "2a", "Radmin")));
        assertEquals(List.of("2a", "2b"), decision.applyRoles(List.of("1alice", "2a", "2c", "2b")));
        assertEquals(List.of(), decision.applyRoles(List.of()));
    }

    // ===== Label =====

    @Test
    public void test_label_displayNameFallsBackToId() {
        assertEquals("Confidential", resolved(ID_CONFIDENTIAL, false, "Confidential", "conf").displayName());
        assertEquals("conf", resolved(ID_CONFIDENTIAL, false, null, " ", "conf").displayName());
        assertEquals(ID_CONFIDENTIAL, resolved(ID_CONFIDENTIAL, false).displayName());
        assertEquals(ID_CONFIDENTIAL, new Label(ID_CONFIDENTIAL, null, null, true).displayName());
        assertEquals(ID_CONFIDENTIAL, Label.unresolved(ID_CONFIDENTIAL).displayName());
    }

    @Test
    public void test_label_displayNameWithParent() {
        assertEquals("Confidential\\All Employees",
                new Label(ID_SECRET, List.of("All Employees", "conf-all"), null, true, ID_CONFIDENTIAL, List.of("Confidential", "conf"))
                        .displayName());
        assertEquals("conf\\" + ID_SECRET,
                new Label(ID_SECRET, List.of(), null, true, ID_CONFIDENTIAL, Arrays.asList(null, " ", "conf")).displayName());
        assertEquals("All Employees",
                new Label(ID_SECRET, List.of("All Employees"), null, true, ID_CONFIDENTIAL, Arrays.asList(" ", null)).displayName(),
                "a parent without a usable name is not shown");
        assertEquals("All Employees", new Label(ID_SECRET, List.of("All Employees"), null, true).displayName());
    }

    @Test
    public void test_label_fourArgConstructorHasNoParent() {
        final Label label = new Label(ID_SECRET, List.of("Secret"), Boolean.TRUE, true);
        assertNull(label.parentId());
        assertEquals(List.of(), label.parentNames());
        assertEquals(new Label(ID_SECRET, List.of("Secret"), Boolean.TRUE, true, null, null), label);
        assertNull(Label.unresolved(ID_SECRET).parentId());
        assertEquals(List.of(), Label.unresolved(ID_SECRET).parentNames());
    }

    @Test
    public void test_label_parentNamesAreCopied() {
        final List<String> parentNames = new ArrayList<>(List.of("Confidential"));
        final Label label = new Label(ID_SECRET, List.of("All Employees"), null, true, ID_CONFIDENTIAL, parentNames);
        parentNames.set(0, "Public");
        assertEquals(List.of("Confidential"), label.parentNames());
        assertThrows(UnsupportedOperationException.class, () -> label.parentNames().add("x"));
    }

    @Test
    public void test_label_namesAreCopied() {
        final List<String> names = new ArrayList<>(List.of("Confidential"));
        final Label label = new Label(ID_CONFIDENTIAL, names, null, true);
        names.set(0, "Public");
        assertEquals(List.of("Confidential"), label.names());
        assertThrows(UnsupportedOperationException.class, () -> label.names().add("x"));
        assertFalse(Label.unresolved(ID_CONFIDENTIAL).resolved());
        assertTrue(Label.unresolved(ID_CONFIDENTIAL).names().isEmpty());
        assertNull(Label.unresolved(ID_CONFIDENTIAL).hasProtection());
    }
}
