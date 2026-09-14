# Severity Matrix

## 1. Severity Framework

The NYC Mobility project uses a severity-based classification framework to prioritize data quality issues according to their impact on analytical reliability, business trust, and release readiness.

Severity classifications support:

- Consistent issue prioritization
- Standardized incident reporting
- Release decision-making
- Escalation management
- Resolution tracking

The framework consists of four severity levels:

```text
Critical
High
Medium
Low
```

---

## 2. Error Classification

### Critical

**Description**

Issues that prevent the creation of a trusted analytical dataset or indicate major processing failures.

**Business Impact**

```text
Reports and analytics cannot be trusted.
```

**Examples**

- Source file missing
- Source ingestion failure
- Gold table not generated
- Pipeline execution failure
- Large-scale duplicate load
- Critical dimension missing

**Required Action**

```text
Stop release immediately.
Root cause analysis required.
Issue must be resolved before release.
```

---

### High

**Description**

Issues that significantly impact analytical accuracy and business confidence.

**Business Impact**

```text
Reports may contain materially incorrect results.
```

**Examples**

- Failed zone mappings
- Large volume of missing records
- Invalid business keys
- Invalid timestamps
- Significant weather join failures
- Significant data loss during transformation

**Required Action**

```text
Investigate immediately.
Resolve before release approval.
```

---

### Medium

**Description**

Issues affecting a subset of records without preventing overall analysis.

**Business Impact**

```text
Analysis remains available but some results may be incomplete.
```

**Examples**

- Partial lookup failures
- Moderate weather join failures
- Missing non-critical fields
- Minor transformation inconsistencies

**Required Action**

```text
Document issue.
Assign owner.
Schedule remediation.
```

---

### Low

**Description**

Issues with limited impact on analytical outcomes.

**Business Impact**

```text
Little or no impact on business reporting.
```

**Examples**

- Formatting inconsistencies
- Metadata issues
- Documentation issues
- Minor naming inconsistencies

**Required Action**

```text
Track for maintenance.
Monitor for recurrence.
```

---

## 3. Escalation Rules

### Critical

**Escalation Timeline**

```text
Immediate
```

**Notify**

```text
Project Owner
Assigned Developer
Project Team
```

**Release Status**

```text
Blocked
```

---

### High

**Escalation Timeline**

```text
Same day
```

**Notify**

```text
Project Owner
Assigned Developer
```

**Release Status**

```text
Review and approval required
```

---

### Medium

**Escalation Timeline**

```text
During current sprint or project cycle
```

**Notify**

```text
Assigned Developer
```

**Release Status**

```text
May proceed with documented risk
```

---

### Low

**Escalation Timeline**

```text
As capacity permits
```

**Notify**

```text
Optional
```

**Release Status**

```text
No release impact
```

---

## 4. Resolution Guidelines

### Critical Issues

**Resolution Requirements**

- Root cause identified
- Corrective action implemented
- Validation re-executed
- Approval obtained before release

**Examples**

```text
Missing source file
Failed Gold load
Fact table corruption
Pipeline failure
```

---

### High Issues

**Resolution Requirements**

- Root cause investigated
- Business impact assessed
- Resolution implemented or approved exception documented
- Validation re-executed if applicable

**Examples**

```text
Failed dimension joins
Invalid timestamps
High volume of null values
```

---

### Medium Issues

**Resolution Requirements**

- Issue logged
- Owner assigned
- Resolution scheduled

**Examples**

```text
Partial lookup mismatches
Partial weather coverage issues
Minor transformation inconsistencies
```

---

### Low Issues

**Resolution Requirements**

- Issue documented
- Monitored for recurrence

**Examples**

```text
Formatting issues
Documentation gaps
Metadata inconsistencies
```

---

## Example Severity Mapping

| Error Scenario | Severity |
|---------------|----------|
| Missing Green Taxi source file | Critical |
| Gold pipeline execution failure | Critical |
| Large duplicate trip load detected | Critical |
| Missing taxi zone mappings | High |
| Invalid trip timestamps | High |
| Weather join coverage below expected threshold | Medium |
| Missing non-critical attribute | Medium |
| Documentation inconsistency | Low |
| Naming convention issue | Low |

---

## Issue Reporting Template

Every identified issue should include:

```text
Issue ID
Date Detected
Dataset
Severity
Description
Business Impact
Owner
Status
Resolution Date
```

Example:

```text
Issue ID: DQ-001
Dataset: fact_green_taxi_trip
Severity: Critical
Description: Duplicate trip records detected
Business Impact: Trip counts inflated
Owner: Data Team
Status: Open
```

---

# Notes / To Be Confirmed

## Severity Thresholds

The project still needs to define exact thresholds for:

```text
Duplicate records

Failed zone joins

Failed weather joins

Missing values

Source completeness

Data loss percentage
```

Example:

```text
0 duplicates = Pass

>0 duplicates = Critical

OR

<0.1% duplicates = Medium
```

Final thresholds should be agreed upon by the team.

---

## Ownership and Escalation Contacts

Confirm responsible owners for:

```text
Critical issues

High issues

Medium issues

Low issues
```

Potential owners:

```text
Project Owner

Assigned Developer

DQ Lead

Entire Project Team
```

---

## Release Blocking Rules

Confirm whether the following should block release:

```text
Critical issues

High issues

Medium issues
```

Not yet finalized.

---

## DQ Dashboard Integration

Confirm whether severity tracking and reporting will be published through:

```text
SQL reporting tables

Dashboard

Notebook reports

Markdown reports

GitHub Issues
```

---

## Traffic Advisory Severity Rules

Confirm severity treatment for traffic advisory data issues if the advisory dataset remains an optional/bonus source.

Potential scenarios:

```text
Missing advisory data

Failed advisory join

Duplicate advisory records
```

---

## Weather Coverage Threshold

Confirm acceptable weather coverage expectations.

Examples:

```text
100% coverage required

95% coverage allowed

90% coverage allowed
```

This decision affects whether failures are classified as Critical, High, or Medium.