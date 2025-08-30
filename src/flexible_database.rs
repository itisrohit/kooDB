use rusqlite::{Connection, Result, types::Value};
use std::collections::HashMap;

// Generic model representation
#[derive(Debug, Clone)]
pub struct Model {
    pub id: Option<i32>,
    pub data: HashMap<String, Value>,
}

// Schema definition for a model type
#[derive(Debug, Clone)]
pub struct Schema {
    pub name: String,
    pub fields: HashMap<String, FieldType>,
}

#[derive(Debug, Clone)]
pub enum FieldType {
    Text,
    Integer,
    Real,
    Boolean,
}

pub struct FlexibleDatabase {
    pub conn: Connection,
    pub schemas: HashMap<String, Schema>,
}

impl FlexibleDatabase {
    pub fn new(db_path: &str) -> Result<FlexibleDatabase> {
        let conn = Connection::open(db_path)?;
        Ok(FlexibleDatabase {
            conn,
            schemas: HashMap::new(),
        })
    }
    
    // Define a ne schema/model type
    pub fn define_schema(&mut self, schema: Schema) -> Result<()> {
        self.schemas.insert(schema.name.clone(), schema.clone());
        
        // Create the table dynamically
        let mut sql = format!("CREATE TABLE IF NOT EXISTS {} (id INTEGER PRIMARY KEY", schema.name);
        
        for (field_name, field_type) in &schema.fields {
            let sql_type = match field_type {
                FieldType::Text => "TEXT",
                FieldType::Integer => "INTEGER",
                FieldType::Real => "REAL",
                FieldType::Boolean => "INTEGER", // SQLite doesn't have boolean, using integer
            };
            
            // Add NOT NULL constraint for all fields except id
            sql.push_str(&format!(", {} {} NOT NULL", field_name, sql_type));
        }
        
        sql.push_str(")");
        
        self.conn.execute(&sql, [])?;
        Ok(())
    }
    
    // Create a new model instance
    pub fn create_model(&self, schema_name: &str, data: HashMap<String, Value>) -> Result<i32> {
        let _schema = self.schemas.get(schema_name)
            .ok_or_else(|| rusqlite::Error::ExecuteReturnedResults)?;
        
        let mut fields = vec![];
        let mut placeholders = vec![];
        let mut values: Vec<Value> = vec![];
        
        for (field_name, value) in data {
            // Validate that field exists in schema
            if !self.schemas.get(schema_name).unwrap().fields.contains_key(&field_name) {
                return Err(rusqlite::Error::ExecuteReturnedResults);
            }
            
            fields.push(field_name);
            placeholders.push("?".to_string());
            values.push(value);
        }
        
        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            schema_name,
            fields.join(", "),
            placeholders.join(", ")
        );
        
        self.conn.execute(&sql, rusqlite::params_from_iter(values))?;
        let id = self.conn.last_insert_rowid() as i32;
        Ok(id)
    }
    
    // Get a model by ID
    pub fn get_model(&self, schema_name: &str, id: i32) -> Result<Option<Model>> {
        let _schema = self.schemas.get(schema_name)
            .ok_or_else(|| rusqlite::Error::ExecuteReturnedResults)?;
        
        let mut sql = format!("SELECT id");
        for field_name in self.schemas.get(schema_name).unwrap().fields.keys() {
            sql.push_str(&format!(", {}", field_name));
        }
        sql.push_str(&format!(" FROM {} WHERE id = ?", schema_name));
        
        let mut stmt = self.conn.prepare(&sql)?;
        let mut rows = stmt.query([id])?;
        
        if let Some(row) = rows.next()? {
            let mut data = HashMap::new();
            let id: i32 = row.get(0)?;
            
            let mut col_index = 1; // Start from 1 because 0 is the id
            for (field_name, field_type) in &self.schemas.get(schema_name).unwrap().fields {
                let value = match field_type {
                    FieldType::Text => Value::Text(row.get(col_index)?),
                    FieldType::Integer => Value::Integer(row.get(col_index)?),
                    FieldType::Real => Value::Real(row.get(col_index)?),
                    FieldType::Boolean => Value::Integer(if row.get::<_, i32>(col_index)? == 0 { 0 } else { 1 }),
                };
                data.insert(field_name.clone(), value);
                col_index += 1;
            }
            
            Ok(Some(Model { id: Some(id), data }))
        } else {
            Ok(None)
        }
    }
    
    // Get all models of a type
    pub fn get_all_models(&self, schema_name: &str) -> Result<Vec<Model>> {
        let _schema = self.schemas.get(schema_name)
            .ok_or_else(|| rusqlite::Error::ExecuteReturnedResults)?;
        
        let mut sql = format!("SELECT id");
        for field_name in self.schemas.get(schema_name).unwrap().fields.keys() {
            sql.push_str(&format!(", {}", field_name));
        }
        sql.push_str(&format!(" FROM {}", schema_name));
        
        let mut stmt = self.conn.prepare(&sql)?;
        let mut rows = stmt.query([])?;
        
        let mut models = Vec::new();
        
        while let Some(row) = rows.next()? {
            let mut data = HashMap::new();
            let id: i32 = row.get(0)?;
            
            let mut col_index = 1; // Start from 1 because 0 is the id
            for (field_name, field_type) in &self.schemas.get(schema_name).unwrap().fields {
                let value = match field_type {
                    FieldType::Text => Value::Text(row.get(col_index)?),
                    FieldType::Integer => Value::Integer(row.get(col_index)?),
                    FieldType::Real => Value::Real(row.get(col_index)?),
                    FieldType::Boolean => Value::Integer(if row.get::<_, i32>(col_index)? == 0 { 0 } else { 1 }),
                };
                data.insert(field_name.clone(), value);
                col_index += 1;
            }
            
            models.push(Model { id: Some(id), data });
        }
        
        Ok(models)
    
    }
    // Update a model
    pub fn update_model(&self, schema_name: &str, id: i32, data: HashMap<String, Value>) -> Result<bool> {
        let _schema = self.schemas.get(schema_name)
            .ok_or_else(|| rusqlite::Error::ExecuteReturnedResults)?;
        
        let mut sets = vec![];
        let mut values: Vec<Value> = vec![];
        
        for (field_name, value) in data {
            // Validate that field exists in schema
            if !self.schemas.get(schema_name).unwrap().fields.contains_key(&field_name) {
                return Err(rusqlite::Error::ExecuteReturnedResults);
            }
            
            sets.push(format!("{} = ?", field_name));
            values.push(value);
        }
        
        // Add the ID to the values for the WHERE clause
        values.push(Value::Integer(id as i64));
        
        if sets.is_empty() {
            return Ok(false);
        }
        
        let sql = format!(
            "UPDATE {} SET {} WHERE id = ?",
            schema_name,
            sets.join(", ")
        );
        
        let rows_affected = self.conn.execute(&sql, rusqlite::params_from_iter(values))?;
        Ok(rows_affected > 0)
    }
    
    // Delete a model
    pub fn delete_model(&self, schema_name: &str, id: i32) -> Result<bool> {
        let _schema = self.schemas.get(schema_name)
            .ok_or_else(|| rusqlite::Error::ExecuteReturnedResults)?;
        
        let sql = format!("DELETE FROM {} WHERE id = ?", schema_name);
        let rows_affected = self.conn.execute(&sql, [id])?;
        Ok(rows_affected > 0)
    }
}