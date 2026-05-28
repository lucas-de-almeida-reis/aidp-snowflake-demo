# Tableau ↔ AIDP

`Demo-AIDP.twb` is a Tableau Desktop workbook that visualises the medallion output against the AIDP cluster over Spark JDBC. The workbook ships with viz logic only — driver, OCI auth, and credentials are local setup.

---

## 1. Install the Simba Spark JDBC driver

Tableau Desktop needs the Simba Spark JDBC driver on the classpath before it will connect to AIDP.

1. Download `SimbaSparkJDBC42-<version>.zip` from Oracle / Simba (the same archive Oracle ships with AIDP samples — version `2.6.18.2069` is known good).
2. Unzip and copy `SparkJDBC42.jar` to Tableau's JDBC drivers folder:
   - **macOS:** `~/Library/Tableau/Drivers/`
   - **Windows:** `C:\Program Files\Tableau\Drivers\`
3. Restart Tableau Desktop.

The driver zip is **not** checked in here — too large, vendor-licensed. Download fresh.

---

## 2. Set up local OCI auth

The JDBC URL points the driver at `~/.oci/config` to sign every request. You need a working API key in OCI.

1. In OCI Console → Identity → Users → your user → **API Keys → Add**. Upload (or generate) a key pair; OCI gives you a `.pem` private key and a fingerprint.
2. Put the `.pem` somewhere stable (e.g. `~/.oci/<your-key>.pem`).
3. Edit `~/.oci/config` so it has at least:
   ```ini
   [DEFAULT]
   user=ocid1.user.oc1..<your-user-ocid>
   fingerprint=<your-fingerprint>
   tenancy=ocid1.tenancy.oc1..<your-tenancy-ocid>
   region=sa-saopaulo-1
   key_file=/absolute/path/to/your-key.pem
   ```

The `.pem` is gitignored at the repo root (`*.pem`). Never check it in.

---

## 3. Build the JDBC URL

Tableau → **Connect → To a Server → Other Databases (JDBC)** → paste this URL:

```
jdbc:spark://gateway.aidp.<region>.oci.oraclecloud.com/default;SparkServerType=AIDP;httpPath=cliservice/<workspace-cliservice-id>;OCIConfigFile=<absolute-path-to-oci-config>;OCIProfile=DEFAULT;UseDatabaseNameAsColumnName=1
```

Filled-in example for this project's workspace:

```
jdbc:spark://gateway.aidp.sa-saopaulo-1.oci.oraclecloud.com/default;SparkServerType=AIDP;httpPath=cliservice/47bf732c-1ccb-4f10-a960-7287ebb4aa77;OCIConfigFile=/Users/lucascoelhodealmeidareis/.oci/config;OCIProfile=DEFAULT;UseDatabaseNameAsColumnName=1
```

**Required parameters:**

| Param | What |
|---|---|
| `gateway.aidp.<region>.oci.oraclecloud.com` | AIDP workspace gateway — region must match the workspace |
| `httpPath=cliservice/<uuid>` | Workspace-specific CLI service ID. Find it in AIDP UI → Workspace → JDBC connection details |
| `OCIConfigFile` | Absolute path to your OCI config (machine-specific) |
| `OCIProfile` | Profile name inside `~/.oci/config` (typically `DEFAULT`) |
| `UseDatabaseNameAsColumnName=1` | **Required.** Without this the Simba driver fails to resolve column names against AIDP catalogs. Oracle's sample URL omits it — add it manually. |

Dialect: **SparkSQL**. Tableau will prompt for username/password — those come from your OCI identity (not stored in the workbook).

---

## 4. Open the workbook

`Demo-AIDP.twb` references the silver and gold tables created by the AIDP notebooks (`silver.default.order_dimensions`, `gold.admin.orders_vertical_performance`). Run the medallion pipeline first; then open the workbook and Tableau will replay the existing data source against your local JDBC connection.

If the connection prompt fails:
- "Couldn't load driver" → `SparkJDBC42.jar` isn't in `~/Library/Tableau/Drivers/`. Reinstall and restart.
- "Column not found" / cryptic field errors → `UseDatabaseNameAsColumnName=1` missing from the URL.
- HTTP 401/403 → check the OCI API key is uploaded for the user in `~/.oci/config`, and the user has access to the AIDP workspace.
