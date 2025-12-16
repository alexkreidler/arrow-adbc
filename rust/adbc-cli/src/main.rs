use std::fs;
use std::io::{self, Write};
use std::time::{Duration, Instant};

use adbc_core::{Connection as _, Database as _, Statement as _};
use adbc_snowflake::{
    database::{AuthType, Builder as DatabaseBuilder},
    driver::Builder as DriverBuilder,
    Database, Driver,
};
use anyhow::{Context, Result};
use arrow_array::RecordBatchReader;
use arrow_schema::DataType;
use clap::{Parser, Subcommand};
use serde::Deserialize;

#[derive(Parser)]
#[command(name = "adbc-cli")]
#[command(about = "ADBC CLI tool for running queries against Snowflake")]
struct Args {
    #[arg(short, long)]
    config: String,

    #[command(subcommand)]
    command: Option<Command>,

    #[arg(short, long)]
    query: Option<String>,

    #[arg(short, long)]
    profile: Option<String>,
}

#[derive(Subcommand)]
enum Command {
    Benchmark {
        #[arg(short, long)]
        query: String,

        #[arg(short, long, default_value = "adbc")]
        client: String,

        #[arg(short, long, default_value = "1")]
        iterations: u32,

        #[arg(short, long)]
        profile: Option<String>,
    },
}

#[derive(Debug, Deserialize)]
struct Config {
    #[serde(flatten)]
    profiles: std::collections::HashMap<String, Profile>,
}

#[derive(Debug, Deserialize)]
struct Profile {
    #[serde(rename = "type")]
    _type: String,
    account: Option<String>,
    user: Option<String>,
    password: Option<String>,
    private_key: Option<String>,
    role: Option<String>,
    warehouse: Option<String>,
    database: Option<String>,
    schema: Option<String>,
    threads: Option<u32>,
    client_session_keep_alive: Option<bool>,
    connect_retries: Option<u32>,
    connect_timeout: Option<u32>,
    retry_on_database_errors: Option<bool>,
    retry_all: Option<bool>,
    reuse_connections: Option<bool>,
}

#[derive(Debug)]
struct BenchmarkResult {
    client: String,
    iterations: u32,
    total_time: Duration,
    avg_time: Duration,
    min_time: Duration,
    max_time: Duration,
    rows: Option<usize>,
}

fn load_config(path: &str) -> Result<Config> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("Failed to read config file: {}", path))?;
    serde_yaml::from_str(&content)
        .with_context(|| format!("Failed to parse config file: {}", path))
}

fn build_database(profile: &Profile) -> Result<(Driver, Database)> {
    let mut driver = DriverBuilder::default()
        .try_load()
        .context("Failed to load Snowflake driver")?;

    let mut db_builder = DatabaseBuilder::default();

    if let Some(account) = &profile.account {
        db_builder = db_builder.with_account(account.clone());
    }

    if let Some(user) = &profile.user {
        db_builder = db_builder.with_username(user.clone());
    }

    if let Some(password) = &profile.password {
        db_builder = db_builder.with_password(password.clone());
    }

    if let Some(private_key) = &profile.private_key {
        db_builder = db_builder
            .with_auth_type(AuthType::Jwt)
            .with_jwt_private_key_pkcs8_value(private_key.trim().to_string());
    }

    if let Some(role) = &profile.role {
        db_builder = db_builder.with_role(role.clone());
    }

    if let Some(warehouse) = &profile.warehouse {
        db_builder = db_builder.with_warehouse(warehouse.clone());
    }

    if let Some(database) = &profile.database {
        db_builder = db_builder.with_database(database.clone());
    }

    if let Some(schema) = &profile.schema {
        db_builder = db_builder.with_schema(schema.clone());
    }

    if let Some(keep_alive) = profile.client_session_keep_alive {
        db_builder = db_builder.with_keep_session_alive(keep_alive);
    }

    let database = db_builder
        .build(&mut driver)
        .context("Failed to build database")?;

    Ok((driver, database))
}

fn format_value(col: &dyn arrow_array::Array, field: &arrow_schema::Field, row_idx: usize) -> String {
    use arrow_array::cast::AsArray;
    
    if col.is_null(row_idx) {
        return "NULL".to_string();
    }
    
    match field.data_type() {
        DataType::Utf8 => col.as_string::<i32>().value(row_idx).to_string(),
        DataType::LargeUtf8 => col.as_string::<i64>().value(row_idx).to_string(),
        DataType::Int8 => col.as_primitive::<arrow_array::types::Int8Type>().value(row_idx).to_string(),
        DataType::Int16 => col.as_primitive::<arrow_array::types::Int16Type>().value(row_idx).to_string(),
        DataType::Int32 => col.as_primitive::<arrow_array::types::Int32Type>().value(row_idx).to_string(),
        DataType::Int64 => col.as_primitive::<arrow_array::types::Int64Type>().value(row_idx).to_string(),
        DataType::UInt8 => col.as_primitive::<arrow_array::types::UInt8Type>().value(row_idx).to_string(),
        DataType::UInt16 => col.as_primitive::<arrow_array::types::UInt16Type>().value(row_idx).to_string(),
        DataType::UInt32 => col.as_primitive::<arrow_array::types::UInt32Type>().value(row_idx).to_string(),
        DataType::UInt64 => col.as_primitive::<arrow_array::types::UInt64Type>().value(row_idx).to_string(),
        DataType::Float32 => col.as_primitive::<arrow_array::types::Float32Type>().value(row_idx).to_string(),
        DataType::Float64 => col.as_primitive::<arrow_array::types::Float64Type>().value(row_idx).to_string(),
        DataType::Boolean => col.as_boolean().value(row_idx).to_string(),
        DataType::Decimal128(_, _) => {
            col.as_primitive::<arrow_array::types::Decimal128Type>().value(row_idx).to_string()
        }
        _ => format!("<{:?}>", field.data_type()),
    }
}

fn print_results(mut reader: impl RecordBatchReader + Send) -> Result<()> {
    let stdout = io::stdout();
    let mut handle = stdout.lock();
    let mut first_batch = true;

    while let Some(batch_result) = reader.next() {
        let batch = batch_result?;
        let schema = batch.schema();

        let num_rows = batch.num_rows();
        let num_cols = batch.num_columns();

        if num_rows == 0 {
            if first_batch {
                writeln!(handle, "Query returned no rows.")?;
            }
            continue;
        }

        let mut col_widths = vec![0; num_cols];
        for (i, field) in schema.fields().iter().enumerate() {
            col_widths[i] = field.name().len().max(10);
        }

        for row_idx in 0..num_rows.min(1000) {
            for col_idx in 0..num_cols {
                let col = batch.column(col_idx);
                let field = schema.field(col_idx);
                let value_str = format_value(col.as_ref(), field, row_idx);
                col_widths[col_idx] = col_widths[col_idx].max(value_str.len());
            }
        }

        for col_idx in 0..num_cols {
            let field = schema.field(col_idx);
            write!(handle, "{:width$} | ", field.name(), width = col_widths[col_idx])?;
        }
        writeln!(handle)?;

        for col_idx in 0..num_cols {
            write!(handle, "{:-<width$}-+-", "", width = col_widths[col_idx])?;
        }
        writeln!(handle)?;

        for row_idx in 0..num_rows.min(1000) {
            for col_idx in 0..num_cols {
                let col = batch.column(col_idx);
                let field = schema.field(col_idx);
                let value_str = format_value(col.as_ref(), field, row_idx);
                write!(handle, "{:width$} | ", value_str, width = col_widths[col_idx])?;
            }
            writeln!(handle)?;
        }

        if num_rows > 1000 {
            writeln!(handle, "\n... (showing first 1000 of {} rows)", num_rows)?;
        }

        first_batch = false;
    }

    Ok(())
}

fn execute_query(database: &Database, query: &str) -> Result<()> {
    let mut connection = database
        .new_connection()
        .context("Failed to create connection")?;

    let mut statement = connection
        .new_statement()
        .context("Failed to create statement")?;

    statement
        .set_sql_query(query)
        .context("Failed to set SQL query")?;

    let reader = statement
        .execute()
        .context("Failed to execute query")?;

    print_results(reader)?;

    Ok(())
}

fn interactive_mode(database: &Database) -> Result<()> {
    println!("ADBC CLI - Interactive Mode");
    println!("Enter SQL queries (or 'exit' to quit):\n");

    loop {
        print!("adbc> ");
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let query = input.trim();

        if query.is_empty() {
            continue;
        }

        if query == "exit" || query == "quit" {
            break;
        }

        match execute_query(database, query) {
            Ok(()) => {}
            Err(e) => eprintln!("Error: {}", e),
        }
    }

    Ok(())
}

async fn benchmark_adbc(profile: &Profile, query: &str, iterations: u32) -> Result<BenchmarkResult> {
    let (_driver, database) = build_database(profile)?;
    
    let mut times = Vec::new();
    let mut total_rows = 0;

    for i in 0..iterations {
        let start = Instant::now();
        
        let mut connection = database
            .new_connection()
            .context("Failed to create connection")?;

        let mut statement = connection
            .new_statement()
            .context("Failed to create statement")?;

        statement
            .set_sql_query(query)
            .context("Failed to set SQL query")?;

        let mut reader = statement
            .execute()
            .context("Failed to execute query")?;

        while let Some(batch_result) = reader.next() {
            let batch = batch_result?;
            total_rows += batch.num_rows();
        }

        let elapsed = start.elapsed();
        times.push(elapsed);
        
        if i == 0 {
            println!("Iteration {}: {:.2?} ({})", i + 1, elapsed, total_rows);
        } else {
            println!("Iteration {}: {:.2?}", i + 1, elapsed);
        }
    }

    let total_time: Duration = times.iter().sum();
    let avg_time = total_time / iterations;
    let min_time = *times.iter().min().unwrap();
    let max_time = *times.iter().max().unwrap();

    Ok(BenchmarkResult {
        client: "adbc".to_string(),
        iterations,
        total_time,
        avg_time,
        min_time,
        max_time,
        rows: Some(total_rows),
    })
}

async fn benchmark_snowflake_connector_rs(
    profile: &Profile,
    query: &str,
    iterations: u32,
) -> Result<BenchmarkResult> {
    use snowflake_connector_rs::{SnowflakeAuthMethod, SnowflakeClient, SnowflakeClientConfig};

    let account = profile.account.as_ref().context("Account is required")?;
    let user = profile.user.as_ref().context("User is required")?;
    
    let auth_method = if let Some(private_key) = &profile.private_key {
        let trimmed_key = private_key.trim();
        if trimmed_key.contains("ENCRYPTED PRIVATE KEY") {
            let key_password = profile.password.as_ref()
                .map(|p| p.as_bytes().to_vec())
                .unwrap_or_default();
            SnowflakeAuthMethod::KeyPair {
                encrypted_pem: trimmed_key.to_string(),
                password: key_password,
            }
        } else if trimmed_key.contains("PRIVATE KEY") {
            return Err(anyhow::anyhow!(
                "snowflake-connector-rs KeyPair authentication requires an encrypted private key (ENCRYPTED PRIVATE KEY). \
                The provided key appears to be unencrypted. Please use an encrypted key or use password authentication instead."
            ));
        } else {
            return Err(anyhow::anyhow!("Invalid private key format"));
        }
    } else if let Some(password) = &profile.password {
        SnowflakeAuthMethod::Password(password.clone())
    } else {
        return Err(anyhow::anyhow!("Either password or private_key is required for authentication"));
    };

    let client = SnowflakeClient::new(
        user,
        auth_method,
        SnowflakeClientConfig {
            account: account.clone(),
            role: profile.role.clone(),
            warehouse: profile.warehouse.clone(),
            database: profile.database.clone(),
            schema: profile.schema.clone(),
            timeout: Some(Duration::from_secs(30)),
        },
    )?;

    let mut times = Vec::new();
    let mut total_rows = 0;

    for i in 0..iterations {
        let start = Instant::now();
        
        let session = client.create_session().await?;
        let rows = session.query(query).await?;
        
        total_rows = rows.len();
        let elapsed = start.elapsed();
        times.push(elapsed);
        
        if i == 0 {
            println!("Iteration {}: {:.2?} ({})", i + 1, elapsed, total_rows);
        } else {
            println!("Iteration {}: {:.2?}", i + 1, elapsed);
        }
    }

    let total_time: Duration = times.iter().sum();
    let avg_time = total_time / iterations;
    let min_time = *times.iter().min().unwrap();
    let max_time = *times.iter().max().unwrap();

    Ok(BenchmarkResult {
        client: "snowflake-connector-rs".to_string(),
        iterations,
        total_time,
        avg_time,
        min_time,
        max_time,
        rows: Some(total_rows),
    })
}

async fn benchmark_snowflake_api_arrow(
    profile: &Profile,
    query: &str,
    iterations: u32,
) -> Result<BenchmarkResult> {
    use snowflake_api::{QueryResult, SnowflakeApi};

    let account = profile.account.as_ref().context("Account is required")?;
    let user = profile.user.as_ref().context("User is required")?;

    let mut times = Vec::new();
    let mut total_rows = 0;

    for i in 0..iterations {
        let start = Instant::now();
        
        let api = if let Some(private_key) = &profile.private_key {
            SnowflakeApi::with_certificate_auth(
                account,
                profile.warehouse.as_deref(),
                profile.database.as_deref(),
                profile.schema.as_deref(),
                user,
                profile.role.as_deref(),
                private_key.trim(),
            )?
        } else if let Some(password) = &profile.password {
            SnowflakeApi::with_password_auth(
                account,
                profile.warehouse.as_deref(),
                profile.database.as_deref(),
                profile.schema.as_deref(),
                user,
                profile.role.as_deref(),
                password,
            )?
        } else {
            return Err(anyhow::anyhow!("Either password or private_key is required for authentication"));
        };

        let result = api.exec(query).await?;
        
        match result {
            QueryResult::Arrow(batches) => {
                for batch in batches {
                    total_rows += batch.num_rows();
                }
            }
            QueryResult::Json(_) => {
                return Err(anyhow::anyhow!("Expected Arrow result but got JSON. Use snowflake-api-json client for JSON results, or ensure your query returns Arrow format (SELECT queries typically return Arrow)"));
            }
            QueryResult::Empty => {
                total_rows = 0;
            }
        }

        let elapsed = start.elapsed();
        times.push(elapsed);
        
        if i == 0 {
            println!("Iteration {}: {:.2?} ({})", i + 1, elapsed, total_rows);
        } else {
            println!("Iteration {}: {:.2?}", i + 1, elapsed);
        }
    }

    let total_time: Duration = times.iter().sum();
    let avg_time = total_time / iterations;
    let min_time = *times.iter().min().unwrap();
    let max_time = *times.iter().max().unwrap();

    Ok(BenchmarkResult {
        client: "snowflake-api-arrow".to_string(),
        iterations,
        total_time,
        avg_time,
        min_time,
        max_time,
        rows: Some(total_rows),
    })
}

async fn benchmark_snowflake_api_json(
    profile: &Profile,
    query: &str,
    iterations: u32,
) -> Result<BenchmarkResult> {
    use snowflake_api::{QueryResult, SnowflakeApi};

    let account = profile.account.as_ref().context("Account is required")?;
    let user = profile.user.as_ref().context("User is required")?;

    let mut times = Vec::new();
    let mut total_rows = 0;

    for i in 0..iterations {
        let start = Instant::now();
        
        let api = if let Some(private_key) = &profile.private_key {
            SnowflakeApi::with_certificate_auth(
                account,
                profile.warehouse.as_deref(),
                profile.database.as_deref(),
                profile.schema.as_deref(),
                user,
                profile.role.as_deref(),
                private_key.trim(),
            )?
        } else if let Some(password) = &profile.password {
            SnowflakeApi::with_password_auth(
                account,
                profile.warehouse.as_deref(),
                profile.database.as_deref(),
                profile.schema.as_deref(),
                user,
                profile.role.as_deref(),
                password,
            )?
        } else {
            return Err(anyhow::anyhow!("Either password or private_key is required for authentication"));
        };

        let result = api.exec(query).await?;
        
        match result {
            QueryResult::Json(json_result) => {
                if let serde_json::Value::Array(rows) = &json_result.value {
                    total_rows = rows.len();
                } else {
                    total_rows = 1;
                }
            }
            QueryResult::Arrow(_) => {
                return Err(anyhow::anyhow!("Expected JSON result but got Arrow. Use snowflake-api-arrow client for Arrow results, or use a non-SELECT query (like SHOW, DESCRIBE) which typically return JSON"));
            }
            QueryResult::Empty => {
                total_rows = 0;
            }
        }

        let elapsed = start.elapsed();
        times.push(elapsed);
        
        if i == 0 {
            println!("Iteration {}: {:.2?} ({})", i + 1, elapsed, total_rows);
        } else {
            println!("Iteration {}: {:.2?}", i + 1, elapsed);
        }
    }

    let total_time: Duration = times.iter().sum();
    let avg_time = total_time / iterations;
    let min_time = *times.iter().min().unwrap();
    let max_time = *times.iter().max().unwrap();

    Ok(BenchmarkResult {
        client: "snowflake-api-json".to_string(),
        iterations,
        total_time,
        avg_time,
        min_time,
        max_time,
        rows: Some(total_rows),
    })
}

fn print_benchmark_result(result: &BenchmarkResult) {
    println!("\n=== Benchmark Results: {} ===", result.client);
    println!("Iterations: {}", result.iterations);
    if let Some(rows) = result.rows {
        println!("Total rows: {}", rows);
    }
    println!("Total time: {:.2?}", result.total_time);
    println!("Average time: {:.2?}", result.avg_time);
    println!("Min time: {:.2?}", result.min_time);
    println!("Max time: {:.2?}", result.max_time);
    println!();
}

async fn run_benchmark(
    config: &Config,
    profile_name: Option<&str>,
    query: &str,
    client: &str,
    iterations: u32,
) -> Result<()> {
    let profile_name = profile_name.unwrap_or("prod");
    let profile = config
        .profiles
        .get(profile_name)
        .with_context(|| format!("Profile '{}' not found in config", profile_name))?;

    println!("Running benchmark with client: {}", client);
    println!("Query: {}", query);
    println!("Iterations: {}\n", iterations);

    let result = match client {
        "adbc" => benchmark_adbc(profile, query, iterations).await?,
        "snowflake-connector-rs" => {
            benchmark_snowflake_connector_rs(profile, query, iterations).await?
        }
        "snowflake-api-arrow" => {
            benchmark_snowflake_api_arrow(profile, query, iterations).await?
        }
        "snowflake-api-json" => {
            benchmark_snowflake_api_json(profile, query, iterations).await?
        }
        _ => {
            return Err(anyhow::anyhow!(
                "Unknown client: {}. Supported clients: adbc, snowflake-connector-rs, snowflake-api-arrow, snowflake-api-json",
                client
            ));
        }
    };

    print_benchmark_result(&result);

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let config = load_config(&args.config)?;

    match args.command {
        Some(Command::Benchmark {
            query,
            client,
            iterations,
            profile,
        }) => {
            run_benchmark(&config, profile.as_deref(), &query, &client, iterations).await?;
        }
        None => {
            let profile_name = args.profile.as_deref().unwrap_or("prod");
            let profile = config
                .profiles
                .get(profile_name)
                .with_context(|| format!("Profile '{}' not found in config", profile_name))?;

            let (_driver, database) = build_database(profile)?;

            if let Some(query) = args.query {
                execute_query(&database, &query)?;
            } else {
                interactive_mode(&database)?;
            }
        }
    }

    Ok(())
}
