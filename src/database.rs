
use polars::prelude::*;
use std::collections::HashMap;
use std::sync::RwLock;
use anyhow::Result;
use polars_sql::SQLContext;

use crate::dataset::Dataset;

#[derive(Eq, Hash, PartialEq)]
pub struct TableName {
    pub schema: String,
    pub name: String,
}

impl TableName {
    pub fn new(schema: String, name: String) -> Self {
        Self {schema, name}
    }

    pub fn handler(&self)-> String {
        // format!("{:?}.{:?}", self.schema, self.name)
        self.name.clone()
    }
}

pub struct Database {
    pub tables: RwLock<HashMap<TableName, Dataset>>, // Underlying parts (referencing to tables)
}

impl Database {
    pub fn new() -> Self {
        let tables = RwLock::new(HashMap::new());
        Self {tables}
    }

    pub fn register(&self, schema: String, name: String, dataset: Dataset) {
        let tn = TableName::new(schema, name);
        (*self.tables.write().unwrap()).insert(tn, dataset);
    }

    pub fn execute_sql(&self, sql: String) -> Result<DataFrame> {
        let mut ctx = SQLContext::new();
        for (tn, ds) in self.tables.read().unwrap().iter() {
            ctx.register(&tn.handler(), ds.to_lazyframe()?);
        };
        let df = ctx.execute(&sql)?.collect()?;
        Ok(df)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_database() -> Result<()> {
        let df = LazyFrame::scan_parquet("data/stock_current/org_key=1/file.parquet", ScanArgsParquet::default())?.with_column(lit(1).alias("org_key")).collect()?; 
        let parts = vec!["org_key".to_string()];
        let buckets = vec!["sku_key".to_string()];
        let ds = Dataset::from_dataframe(df.clone(), Some(parts), Some(buckets), None)?;

        let db = Database::new();
        db.register("public".to_string(), "stock_current".to_string(), ds);

        let df = db.execute_sql("SELECT * FROM stock_current WHERE store_key = 101;".to_string())?;
        println!("{:?}", df);

        Ok(())
    }
}