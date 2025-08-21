use rusqlite::{{Connection, Result ,types::Value}};

use std::Collection::HashMap;


// Generic model representation for a model type 
#[derive(Debug, Clone)]
pub struct Model {
    pub id: Option<i32>,
    pub data: Hashmap<String, Value>,
}


// Schema defination for model type 
#[derive(Debug, Clone)]
public struct Schema {
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

    // Define a new schema/model type 
    
    pub fn define_schema(&mut self, schema: Schema) -> Result<()> {
        self.schemas.insert(schema.name.clone(), schema.clone());

        // Create table Dynamically
        
        let mut sql = format!("CREATE TABLE IF NOT EXISTS {} (id INTEGER PRIMARY KEY", schema.name);
        for (field_name, field_type) in &schema.fields {
            let sql_type = match field_type {
                FieldType::Text => "TEXT",
                FieldType::Integer => "INTEGER",
                FieldType::Real => "REAL",
                FieldType::Boolean => "BOOLEAN",
            };

            // Add NOT NULL constraint for all fields except id
            sql.push_str(&format!(", {} {} NOT NULL", field_name, sql_type));
        }

        sql.push_str(")");

        self.conn.excecute(&sql, [])?;
        Ok(())
}

// Create a new model instance
pub fn create_model(&self, schema_name: &str, data: HashMap<String, Value>) -> Result<i32> {
    let _schema = self.schemas.get(schema_name)
        .ok_or_else(|| rusqlite:: Error::ExecuteReturnedResults)?;

    let mut fields = vec![];
    let mut placeholders = vec![];
    let mut values: Vec<Value> = vec![];

    for (field_name, value) in data {
        // Validate that field exists in schema
        if !self.schemas.get(schema_name).unwarp().fields.contains_key(&field_name) {
            return Err(ruqslite::Error::ExcecuteReturnedResults);
        }

        fields.push(field_name);
        placeholders.push("?".to_string());
        values.push(value);
    }

    let sql = format!(
        "INSERT INTO {} ({}) VALUES ({})"),
        schema_name,
        fields.join(", "),
        placeholders.join(", ")
    );

    self.conn.execute(&sql, rusqlite::params_from_iter(values.iter()))?;
    let id = self.conn.last_insert_rowid() as i32;
    Ok(id)
}

// Get model by id

