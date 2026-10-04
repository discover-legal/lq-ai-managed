# Managed LQ.AI — platform decisions

Decision log for the managed (MSP) offering built on this fork of
[LegalQuants/lq-ai](https://github.com/LegalQuants/lq-ai). Each entry records
the choice made by the owner and its known consequences. Open items are listed
at the end.

## 0. Hosted cloud
**Azure**, with **Azure OpenAI** for GPT models (brand preference for OpenAI
models; keeps GPT traffic in-cloud and in-region/zone).

## 1. Tenancy model
**Silo per client** — one complete LQ.AI install per client (own database, file
store, keys). Upstream LQ.AI is single-organization per install; no app-level
multi-tenancy is added.

## 2. Offerings / tiers
| Tier | Where it runs |
|---|---|
| **Shared** | MSP's Entra tenant; shared subscription; resource group + Container Apps environment per client |
| **Dedicated subscription** | MSP's Entra tenant; subscription per client |
| **Dedicated tenant** | New Entra tenant per client, owned by the MSP, managed via Azure Lighthouse |
| **DIY templates** | Buyer's own cloud, buyer-deployed — **Azure, AWS and GCP** — sold from launch |

## 3. Compute
- Shared: **Azure Container Apps** (workload-profiles environment — needed for UDR/NAT egress), environment per client.
- Dedicated subscription / tenant: **AKS**, cluster per client.
- DIY (Azure): **Container Apps**. AWS/GCP DIY equivalents TBD.
- Consequence: two deployment formats (Container Apps + Helm/AKS); upstream Helm chart must be completed (workers missing, DE-327).

## 4. Database and cache
- Postgres (all hosted tiers): **Azure Database for PostgreSQL Flexible Server, one per client**. Extensions `vector`, `citext`, `pgcrypto` allow-listed.
- DIY: buyer chooses **container or Flexible Server** (Azure-managed, not MSP-managed). Container option documented as buyer-owned backups; network-share caveat.
- Redis: **container** for Shared + DIY; **Azure Managed Redis** for dedicated tiers. Test arq job-loss recovery on container restart.

## 5. File storage
**Native Azure Blob adapter**, built in this fork **for upstream contribution** (see 15) replacing the S3 path in `api/app/storage.py`. Gains: per-account keys, immutability/legal hold, soft delete, versioning, lifecycle. Carry cost: merge upstream `storage.py` changes and future object-store ops migrations; Azurite-based tests.

## 6. Sign-in
- **Generic OIDC** in the fork (Entra, Google Workspace, Okta, …).
- **Local accounts off entirely** — needs a first-admin bootstrap (configured admin email/group).
- Offboarding: **SCIM** + **re-check with IdP on token refresh** (closes the 7-day refresh-token window).

## 7. Models
- Providers: **Azure OpenAI / Foundry**, **Claude via Foundry**, **local GPU models** (add-on).
- Billing: **MSP resells by default; BYOK optional** per client.
- Azure OpenAI: **resource per client**. Shared-tier clients share subscription quota.
- **Apply for modified abuse monitoring** (no prompt retention); **keyless managed-identity auth** to be built in the fork.
- Fork work: verify/adapt gateway Anthropic adapter for Claude via Foundry.

## 8. Regions and residency
- Markets: **Canada, US, EU**.
- Model processing: **in-zone** — Data Zone in US/EU; regional deployments in Canada (narrower model set; Claude via Foundry may be unavailable in Canada — verify).
- DR: **same country**; EU DR **per client** (same country vs same EU data zone, with the trade-off disclosed).

## 9. Network
- Entry: **Application Gateway + WAF only** (TLS terminates in-region). **Shared across Shared-tier clients per region; per client on dedicated tiers.**
- Data services: **VNet integration / service endpoints** by default on all tiers; Shared tier **switchable to private endpoints** per client. Postgres networking mode fixed at creation → **asked at onboarding** for Shared tier.
- Egress: Shared — **NSG + NAT gateway per client**; Dedicated — **Azure Firewall Standard per client**.

## 10. Encryption keys
- Shared: **MSP-held customer-managed key per client** (enables crypto-shred).
- Dedicated: **client-held key** in the client's own Key Vault (cross-tenant CMK; verify Postgres support).
- Key store: **Key Vault Premium** (HSM-backed keys).
- Offboarding: **crypto-shred** — Shared: MSP deletes key after export window; Dedicated: MSP deletes resources, client deletes key and confirms in writing.
- LQ.AI application master keys (Fernet etc.) sourced from the client's Key Vault.
- CMK must be set at Postgres creation.

## 11. Staff access
Sized for a one-to-two-person operation at launch.
- Infrastructure (control plane): **standing access**. Accepted consequence: standing Owner/Contributor can technically reach data (password resets, key listing, role grants).
- Data (content): **client-approved** — procedural, not technically enforced on the Shared tier; dedicated tiers additionally gated by client-held keys.
- App: **standing support account** per install, signing in as a **guest in the client's IdP** (Entra B2B = one identity for M365 clients; Google/Okta clients create a full account). App role set during build.
- Break-glass: emergency accounts per tenant, **credentials sealed, internal alert on use**.

## 12. Logs and monitoring
- **Split logging + client copy.** Content-free operational telemetry (timings, error types, IDs, counts) → an **Azure Monitor workspace per client**, operated by the MSP. Detailed logs (may contain document text/prompts) → storage **inside the client's environment, encrypted with the client's data key** (covered by crypto-shred), readable only via the client-approval process. Retention of detailed logs **chosen by the client** at onboarding. Clients may have copies forwarded to their own SIEM/storage.
- Implementation: per-install OpenTelemetry collector with two pipelines (filtered → Azure Monitor; full → client Blob). Verify maturity of the collector's Blob exporter or use a sidecar.
- **LQ.AI audit log** exported to an **immutable (WORM) Blob container** in the client's environment by default, or to the **client's SIEM** if provided (small exporter in the fork).
- **Azure activity logs** (incl. MSP staff actions): **kept internal**, provided on request.
- No customer-managed-key log cluster (cost); content is kept out of Azure Monitor instead.

## - Included baseline: **backup and restore** — Postgres geo-redundant backup (must be enabled at server creation; CMK key must exist in the paired region, client-provided on dedicated tiers), Blob soft delete/versioning; rebuild in the paired region via Terraform. Typical RPO ≤ ~1h, RTO hours–a day.
- **Warm standby** (cross-region replica, pre-built infra): **paid add-on**.
- **Zone redundancy** (Postgres zone-redundant HA, zone-redundant compute/storage): **paid add-on**. Note: Container Apps environment and AKS node-pool zone settings are fixed at creation — ask at onboarding or plan a rebuild.
- Retention (PITR window / long-term copies): **client chooses** at onboarding within supported limits (PITR 7–35 days; long-term via Azure Backup or immutable dumps).
- **Restore testing: annually**, with evidence kept.
- Verify every required service exists in each DR region (e.g. Container Apps in Canada East).

## 14. Infrastructure as code and deployment
- **Terraform**, run from **GitHub Actions** in this fork; **state in an MSP-owned Azure Storage account**, one state file per client per stack, with locking.
- Apps: **Terraform push** to both Container Apps and AKS (no GitOps controller).
- Images built from this fork into a **container registry per client** (copy step per client; Basic ACR on Shared has no private networking — size SKU per tier).
- **Sign images and enforce**: AKS via image-integrity policy (Ratify/Azure Policy); Container Apps has no native admission check — enforce by verifying signatures in the deploy pipeline.

## 15. Tracking upstream
- **Weekly merge of upstream `main` into a staging branch**; clients receive **upstream tagged releases** (plus expedited security fixes).
- Fork changes structured as **modules + small hooks**: interfaces with new per-backend/provider files (e.g. `STORAGE_BACKEND=s3|azureblob`, pluggable auth provider, gateway adapters, audit exporter), touching as few upstream lines as possible; documented via ADRs.
- Rollout in **rings** (test → internal → Shared → dedicated) with a **monthly maintenance window**; security fixes expedited.
- **Contribute everything upstream, including the Blob adapter**, following upstream's CONTRIBUTING/DCO/ADR process.

## - **Branding: co-brand** — MSP brand + "powered by LQ.AI"; **Open WebUI branding left intact** (web/LICENSE clause 4 forbids removal above 50 end users per deployment without permission/enterprise licence); LQ.AI trademark owned by LegalQuants — nominative use only without permission.
- **Platform: base fee + per seat.** Base covers the per-client infra floor (rough, before model usage: Shared ~US$130–200/mo; Dedicated ~US$1,800–2,200/mo, dominated by Firewall Standard + App Gateway).
- **Resold model usage: cost + markup**, metered at the gateway. BYOK clients pay platform only.
- **DIY templates: free lead magnet.**
- Paid add-ons: warm standby DR, zone redundancy, local GPU models, private endpoints (Shared).

## 16b. Support and SLA
- Uptime: **best effort, no formal SLA** at launch (single-zone baseline composite ≈ 99.8% from component SLAs).
- Support: **business hours + best-effort after-hours on-call for critical outages**.
- **No service credits.**

## 17. Compliance
- **No certification at launch.** Later targets: **ISO 27001**, **ISO 27701**, **ISO 42001**.
- At launch: **DPA + published sub-processor list** (GDPR Art. 28, PIPEDA, Quebec Law 25).
- **Compliance automation platform** (Vanta/Drata/Secureframe class) for policies and evidence.
- Auditor-sensitive choices to justify when certifying: standing admin access (11), internal-only activity logs (12), annual restore tests (13).
- Upstream `docs/compliance/` alignment docs are stubs — authoring them advances certification and goes upstream (15).

## Build backlog derived from these decisions

### Fork (app) changes — modules + hooks, contributed upstream
1. Storage backend interface + **Azure Blob adapter** (`STORAGE_BACKEND=s3|azureblob`), Azurite tests.
2. **Generic OIDC sign-in**; local accounts off; first-admin bootstrap; **IdP re-check on refresh**; **SCIM** provisioning.
3. Gateway: **Azure OpenAI managed-identity auth**; **Claude via Foundry** adapter/verification.
4. **Audit-log exporter** (immutable Blob / client SIEM).
5. App secrets (Fernet master keys etc.) sourced from **Key Vault**.
6. **Trusted-proxy client IP** handling (Application Gateway in front; today the API logs the proxy IP).
7. **Helm chart** completion: ingest/arq workers + single migration job (DE-327).
8. Compliance alignment docs (SOC 2 / ISO 27001 / 42001 / GDPR) to replace upstream stubs.

### Infrastructure (Terraform, GitHub Actions)
9. Platform: state storage, GitHub Actions pipelines, image build + signing, per-client registries.
10. Shared-tier module: Container Apps (workload profiles), Postgres (CMK + geo-backup + networking chosen at onboarding), Blob, Key Vault Premium, Redis container, NAT, shared App Gateway/WAF per region, Azure Monitor workspace, OTel split pipelines, Azure OpenAI resource.
11. Dedicated module: AKS, Firewall Standard, App Gateway/WAF, Managed Redis, client-held CMK, same data services; dedicated-tenant bootstrap via Lighthouse.
12. Add-ons: warm standby, zone redundancy, private endpoints, local GPU.
13. DIY templates: Azure, AWS, GCP.

### Verify before committing
- Claude via Foundry availability/residency in Canadian regions.
- Cross-tenant CMK support for Postgres Flexible Server.
- Container Apps and all required services in each DR region (e.g. Canada East).
- OTel collector Blob exporter maturity.
- arq job recovery after Redis container restart.
- Legal review: Open WebUI clause 4 interpretation per deployment; LQ.AI trademark use.
- MCA billing for subscriptions in MSP-created tenants.
- Azure OpenAI modified abuse monitoring application.
