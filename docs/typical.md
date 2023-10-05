# Typical ETL use case

## Context
* typical use is small to medium size Master Data Management: validating & loading historical data
* data volume is typically small to medium
* data validation is paramount
* history & audit trail are the most prominent features
* we don't focus here on the consuming side: extracting data to produce report is not the primary focus of this library (might evolve later on)

## Objectives
* low on resources, lightweight ETL logic
* reusable, composable parts
* approach: build-you-own using a simple toolkit rather than learn a complex one-size-fits-all platform that would eventually require heavy customization
* the proposed toolkit should enable developers to quickly build secure, production-ready integration jobs, with a focus on their business rules: validation, etc.
* possibility to deal with a few large datasets whenever they come in, without too much hassle - by default however, setup should be easy for small to medium workloads

> Small: a few thousands rows input
> Medium: about 1M rows input
> Large: about 10M rows input
> Extra-large: about 100M rows input

## Typical ETL job

I am a strong advocate of coding such repetitive jobs with a general purpose language (such as Python),
hopefully equipped with easy-to-use table & file handling features (such as data frames).

Experience showed that zero-code/low-code solution are rapidly vastly demanding in terms of expertise,
usually with a narrow pool of available resources actually able to twist such products to the needs.

Many existing solutions can deal with job orchestration and handle job triggers, so this is a non-goal for our framework.

## Desirable properties (principles)

The core principles are: exploitability, observability, testability.

### Exploitability

Do:
A job is exploitable in perfect conditions if it is:
*	Deployable (automated, without complex coordination)
*	Idempotent (i.e. can be safely restarted)
*	Has limited dependencies (i.e. “standalone”)
*	Robust
*	Resilient
*	Performant (more precisely: cheap to compute)

Besides, any MDM data integration should be able to:
*	Safely restate history of any past record

Don't:
*	Clumsy/complex/manual deployment
*	Need cleanup before restarting
*	Need a precise order of pre-requisite jobs
*	Prone to failures due to a slight change
*	No recovery from failures, need clean-up
*	Minutes to load a few lines, multi-nodes cluster to process a petty 100k lines
*	Requires a full cleanup and full reload when it comes to restating the past
  (e.g. recovering from data corruption, or simply restatement of wrong pdata in the past)

### Observability

*	The deployed version of any component is clearly visible
*	Data feeds and jobs can be easily navigated through: control flows, triggers and dependencies are visible by production operators AND developers
*	The status of a job is visible from the end user (i.e. app portal)
*	And so are basic statistics of a job (size, timings, metrics about the different actions carried out)
*	And so are data changes (audit trail), and why they have changed or failed to change
*	The status & logs of a job are visible from production operators AND developers
*	Metrics (timings, volumes) about runs can be navigated by production operators AND developers

### Testability
A job that cannot be properly tested is of only limited value. Design for testability and… write tests.

**A design that is not testable is not a great design.**

*	Unit testing
  *	Unit tests MANDATORILY capture at least 1 happy path
  * Unit testing is great to explore code paths and test edge cases – use mocks

Scripting languages routinely require a higher test coverage than their compiled brethren.
  * CI learning: every bug fix should come with a repro test case
  
* Integration testing
  * Integration tests systematically run on CI all DB migration scripts (and rollbacks)
  * Modularized integration test suites populate the fresh DB with minimal test data (i.e. taxos, baseline history…)
  * Integration testing MANDATORILY captures at least 1 happy path against a real database
  * Integration tests are not here to exercise exception handling (see unit tests)

## Features

* collect parameterization
* validate input against schema
* identify entities
* map nomenclatures/taxonomies/typologies (same thing)
* collect recyclable items
* map attributes to target entities
* load / arbitrage historical record of entities
* assert data quality rules
* record audit trail

## Kinds of ETL jobs

Typically, they fall into one of these categories:
*	Extract-only (input producers, e.g. intermediate data copy activity) [should be avoided whenever not strictly required]
*	Extract-only (output producers)
*	Assert-only (notifications producers)
*	Transform-and-extract (purge & archival jobs)
