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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codelibs.core.lang.StringUtil;

/**
 * The per-label rules configured by the {@code sensitivity_label_policy.<label>} parameters, and
 * the decision they produce for one file.
 *
 * <p>Each rule is {@code <label>=<action>[;<action>...]}; {@link #parse} takes them one per line,
 * and ignores blank lines and lines starting with {@code #}. {@code <label>} is one of:</p>
 * <ul>
 * <li>a label ID (GUID);</li>
 * <li>a label name or display name, compared case-insensitively;</li>
 * <li>{@value #PROTECTED_LABEL}: any label that applies encryption;</li>
 * <li>{@value #ANY_LABEL}: any label not matched by a more specific rule.</li>
 * </ul>
 * <p>A file carries the ID of a sublabel, never that of its parent, so a rule for a parent label
 * also applies to its sublabels. A label is matched by its own ID, its own names, its parent's
 * ID, its parent's names, {@value #PROTECTED_LABEL} and {@value #ANY_LABEL}, in that order, and
 * only the first matching rule applies to it. The actions are:</p>
 * <ul>
 * <li>{@value #ACTION_INDEX}: index the file as usual; used to exempt a label from a broader rule;</li>
 * <li>{@value #ACTION_SKIP}: do not index the file;</li>
 * <li>{@value #ACTION_NO_CONTENT}: index the file without downloading its content;</li>
 * <li>{@value #ACTION_RESTRICT}{@code :<permissions>}: keep only the roles of the file's ACL that
 * are also listed here, in the {@code default_permissions} syntax. This narrows the ACL and never
 * widens it.</li>
 * </ul>
 *
 * <p>When no {@value #PROTECTED_LABEL} rule is configured, a label that applies encryption gets
 * the {@value #ANY_LABEL} rule, if any, with {@value #ACTION_NO_CONTENT} added: Microsoft Graph
 * returns such a file still encrypted, so downloading it only produces an extraction failure.</p>
 *
 * <p>A file can carry several labels. Their rules are combined so that the result is never less
 * restrictive than any one of them: {@value #ACTION_SKIP} and {@value #ACTION_NO_CONTENT} apply if
 * any label asks for them, and the {@value #ACTION_RESTRICT} lists are intersected.</p>
 *
 * <p>Instances are immutable and safe to share between crawler threads.</p>
 */
public class SensitivityLabelPolicy {

    /** The rule key that matches any label not matched by a more specific rule. */
    public static final String ANY_LABEL = "*";

    /** The rule key that matches any label that applies encryption. */
    public static final String PROTECTED_LABEL = "@protected";

    /** The action that indexes the file as usual. */
    public static final String ACTION_INDEX = "index";

    /** The action that excludes the file from the index. */
    public static final String ACTION_SKIP = "skip";

    /** The action that indexes the file without its content. */
    public static final String ACTION_NO_CONTENT = "no_content";

    /** The action that narrows the file's roles to the listed permissions. */
    public static final String ACTION_RESTRICT = "restrict";

    /** The rule applied to a label that applies encryption when no {@value #PROTECTED_LABEL} rule is configured. */
    static final Rule DEFAULT_PROTECTED_RULE = new Rule(false, true, null);

    /** Rules keyed by lower-cased label ID or name. */
    private final Map<String, Rule> labelRules;

    /** The {@value #PROTECTED_LABEL} rule, or {@code null} when it is not configured. */
    private final Rule protectedRule;

    /** The {@value #ANY_LABEL} rule, or {@code null} when it is not configured. */
    private final Rule anyRule;

    /** Whether any rule other than {@value #ANY_LABEL} can match a label through its definition. */
    private final boolean definitionRequired;

    private SensitivityLabelPolicy(final Map<String, Rule> labelRules, final Rule protectedRule, final Rule anyRule) {
        this.labelRules = labelRules;
        this.protectedRule = protectedRule;
        this.anyRule = anyRule;
        // A label ID rule needs the definition too: it can match a sublabel through its parent.
        definitionRequired = protectedRule != null || !labelRules.isEmpty();
    }

    /**
     * Parses sensitivity label rules.
     *
     * @param value the rules, one {@code <label>=<action>[;<action>...]} per line; blank means no rules
     * @param permissionEncoder encodes one {@code default_permissions}-style entry into a search role
     * @return the parsed policy
     * @throws IllegalArgumentException if a line is malformed, names an unknown action or repeats a label
     */
    public static SensitivityLabelPolicy parse(final String value, final Function<String, String> permissionEncoder) {
        final Map<String, Rule> labelRules = new HashMap<>();
        Rule protectedRule = null;
        Rule anyRule = null;
        if (StringUtil.isNotBlank(value)) {
            for (final String rawLine : value.split("\\r?\\n")) {
                final String line = rawLine.trim();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                final int pos = line.indexOf('=');
                if (pos <= 0) {
                    throw new IllegalArgumentException("Expected <label>=<action>, but got: " + line);
                }
                final String key = line.substring(0, pos).trim();
                final Rule rule = parseRule(line.substring(pos + 1), permissionEncoder, line);
                if (key.isEmpty()) {
                    throw new IllegalArgumentException("A label is required: " + line);
                }
                if (ANY_LABEL.equals(key)) {
                    if (anyRule != null) {
                        throw new IllegalArgumentException("Duplicate rule for " + key);
                    }
                    anyRule = rule;
                } else if (PROTECTED_LABEL.equalsIgnoreCase(key)) {
                    if (protectedRule != null) {
                        throw new IllegalArgumentException("Duplicate rule for " + key);
                    }
                    protectedRule = rule;
                } else if (labelRules.putIfAbsent(key.toLowerCase(Locale.ROOT), rule) != null) {
                    throw new IllegalArgumentException("Duplicate rule for " + key);
                }
            }
        }
        return new SensitivityLabelPolicy(labelRules, protectedRule, anyRule);
    }

    private static Rule parseRule(final String value, final Function<String, String> permissionEncoder, final String line) {
        boolean skip = false;
        boolean noContent = false;
        Set<String> restrictRoles = null;
        boolean hasAction = false;
        for (final String rawAction : value.split(";")) {
            final String action = rawAction.trim();
            if (action.isEmpty()) {
                continue;
            }
            hasAction = true;
            final int pos = action.indexOf(':');
            final String name = (pos < 0 ? action : action.substring(0, pos)).trim().toLowerCase(Locale.ROOT);
            if (ACTION_RESTRICT.equals(name)) {
                if (restrictRoles != null) {
                    throw new IllegalArgumentException("Duplicate " + ACTION_RESTRICT + " action: " + line);
                }
                final Set<String> roles = new LinkedHashSet<>();
                if (pos >= 0) {
                    for (final String permission : action.substring(pos + 1).split(",")) {
                        if (StringUtil.isNotBlank(permission)) {
                            final String role = permissionEncoder.apply(permission.trim());
                            if (StringUtil.isNotBlank(role)) {
                                roles.add(role);
                            }
                        }
                    }
                }
                if (roles.isEmpty()) {
                    throw new IllegalArgumentException(ACTION_RESTRICT + " requires at least one permission: " + line);
                }
                restrictRoles = Collections.unmodifiableSet(roles);
            } else if (pos >= 0) {
                throw new IllegalArgumentException("Action " + name + " takes no argument: " + line);
            } else if (ACTION_SKIP.equals(name)) {
                skip = true;
            } else if (ACTION_NO_CONTENT.equals(name)) {
                noContent = true;
            } else if (!ACTION_INDEX.equals(name)) {
                throw new IllegalArgumentException("Unknown action " + name + ": " + line);
            }
        }
        if (!hasAction) {
            throw new IllegalArgumentException("An action is required: " + line);
        }
        return new Rule(skip, noContent, restrictRoles);
    }

    /**
     * Returns whether the rule for {@code label} can be determined.
     *
     * <p>It cannot when the label's definition could not be read, no rule names the label's own
     * ID, and some rule other than {@value #ANY_LABEL} is configured: that rule might have matched
     * the label by name, through its parent or by {@value #PROTECTED_LABEL}, so applying a broader
     * rule - or none - would fail open.</p>
     *
     * @param label the label to check
     * @return {@code false} if the label's rule depends on a definition that is not available
     */
    public boolean canEvaluate(final Label label) {
        if (label.resolved() || !definitionRequired) {
            return true;
        }
        return label.id() != null && labelRules.containsKey(label.id().toLowerCase(Locale.ROOT));
    }

    /**
     * Finds the rule that applies to one label.
     *
     * @param label the label
     * @return the matching rule, or {@code null} when no rule applies
     */
    public Rule findRule(final Label label) {
        if (label.id() != null) {
            final Rule rule = labelRules.get(label.id().toLowerCase(Locale.ROOT));
            if (rule != null) {
                return rule;
            }
        }
        if (label.resolved()) {
            Rule rule = findRuleByName(label.names());
            if (rule != null) {
                return rule;
            }
            if (label.parentId() != null) {
                rule = labelRules.get(label.parentId().toLowerCase(Locale.ROOT));
                if (rule != null) {
                    return rule;
                }
            }
            rule = findRuleByName(label.parentNames());
            if (rule != null) {
                return rule;
            }
            if (Boolean.TRUE.equals(label.hasProtection())) {
                if (protectedRule != null) {
                    return protectedRule;
                }
                // The default must not shadow the * rule: an operator who wrote *=skip means
                // every labeled file, encrypted or not.
                return anyRule != null ? new Rule(anyRule.skip(), true, anyRule.restrictRoles()) : DEFAULT_PROTECTED_RULE;
            }
        }
        return anyRule;
    }

    private Rule findRuleByName(final List<String> names) {
        for (final String name : names) {
            if (StringUtil.isNotBlank(name)) {
                final Rule rule = labelRules.get(name.toLowerCase(Locale.ROOT));
                if (rule != null) {
                    return rule;
                }
            }
        }
        return null;
    }

    /**
     * Combines the rules of every label on a file into one decision.
     *
     * <p>Callers must check {@link #canEvaluate(Label)} for each label first.</p>
     *
     * @param labels the labels on the file; empty for an unlabeled file
     * @return the decision for the file
     */
    public Decision decide(final List<Label> labels) {
        boolean skip = false;
        boolean noContent = false;
        Set<String> allowedRoles = null;
        for (final Label label : labels) {
            final Rule rule = findRule(label);
            if (rule == null) {
                continue;
            }
            skip |= rule.skip();
            noContent |= rule.noContent();
            if (rule.restrictRoles() != null) {
                if (allowedRoles == null) {
                    allowedRoles = new LinkedHashSet<>(rule.restrictRoles());
                } else {
                    allowedRoles.retainAll(rule.restrictRoles());
                }
            }
        }
        return new Decision(skip, noContent, allowedRoles == null ? null : Collections.unmodifiableSet(allowedRoles));
    }

    /**
     * Returns whether no rule is configured, so only the default rule for encrypted labels applies.
     *
     * @return {@code true} when no rule was given
     */
    public boolean isEmpty() {
        return labelRules.isEmpty() && protectedRule == null && anyRule == null;
    }

    /**
     * A sensitivity label found on a file, together with what is known of its definition.
     *
     * @param id the label ID
     * @param names the label's name and display name; empty when the definition could not be read
     * @param hasProtection whether the label applies encryption; {@code null} when unknown
     * @param resolved whether the label's definition was read
     * @param parentId the parent label's ID for a sublabel, otherwise {@code null}
     * @param parentNames the parent label's name and display name; empty for a top-level label
     */
    public record Label(String id, List<String> names, Boolean hasProtection, boolean resolved, String parentId, List<String> parentNames) {

        /**
         * Creates a label.
         *
         * @param id the label ID
         * @param names the label's name and display name
         * @param hasProtection whether the label applies encryption
         * @param resolved whether the label's definition was read
         * @param parentId the parent label's ID for a sublabel
         * @param parentNames the parent label's name and display name
         */
        public Label {
            names = names == null ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(names));
            parentNames = parentNames == null ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(parentNames));
        }

        /**
         * Creates a top-level label.
         *
         * @param id the label ID
         * @param names the label's name and display name
         * @param hasProtection whether the label applies encryption
         * @param resolved whether the label's definition was read
         */
        public Label(final String id, final List<String> names, final Boolean hasProtection, final boolean resolved) {
            this(id, names, hasProtection, resolved, null, null);
        }

        /**
         * Creates a label whose definition could not be read.
         *
         * @param id the label ID
         * @return the label
         */
        public static Label unresolved(final String id) {
            return new Label(id, Collections.emptyList(), null, false);
        }

        /**
         * Returns the name to show for the label: the first non-blank name, or the ID. A sublabel
         * is shown under its parent, as {@code Parent\Sublabel}, the way Office shows it.
         *
         * @return the display name
         */
        public String displayName() {
            final String name = names.stream().filter(StringUtil::isNotBlank).findFirst().orElse(id);
            return parentNames.stream().filter(StringUtil::isNotBlank).findFirst().map(parent -> parent + "\\" + name).orElse(name);
        }
    }

    /**
     * The actions configured for one label.
     *
     * @param skip whether the file is not indexed
     * @param noContent whether the file is indexed without its content
     * @param restrictRoles the roles the file's ACL is narrowed to, or {@code null} for no restriction
     */
    public record Rule(boolean skip, boolean noContent, Set<String> restrictRoles) {
    }

    /**
     * What to do with one file, after combining the rules of all its labels.
     *
     * @param skip whether the file is not indexed
     * @param noContent whether the file is indexed without its content
     * @param allowedRoles the roles the file's ACL is narrowed to, or {@code null} for no restriction
     */
    public record Decision(boolean skip, boolean noContent, Set<String> allowedRoles) {

        /** The decision for a file no rule applies to. */
        public static final Decision NONE = new Decision(false, false, null);

        /**
         * Narrows a file's roles to {@link #allowedRoles()}.
         *
         * @param roles the roles collected from the file's ACL and the configured defaults
         * @return the roles that are also allowed, in their original order; {@code roles} itself when there is no restriction
         */
        public List<String> applyRoles(final List<String> roles) {
            if (allowedRoles == null) {
                return roles;
            }
            return roles.stream().filter(allowedRoles::contains).collect(Collectors.toList());
        }
    }
}
