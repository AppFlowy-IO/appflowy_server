use app_error::AppError;
use appflowy_collaborate::collab::storage::CollabAccessControlStorage;
use collab::core::collab::DataSource;
use collab::preclude::Collab;
use collab_database::database::DatabaseBody;
use collab_database::entity::FieldType;
use collab_database::fields::type_option_cell_reader;
use collab_database::fields::Field;
use collab_database::fields::TypeOptionCellReader;
use collab_database::fields::TypeOptionData;
use collab_database::fields::TypeOptions;
use collab_database::rows::new_cell_builder;
use collab_database::rows::Cell;
use collab_database::rows::Cells;
use collab_database::template::entity::CELL_DATA;
use collab_database::workspace_database::NoPersistenceDatabaseCollabService;
use collab_entity::CollabType;
use collab_entity::EncodedCollab;
use collab_folder::CollabOrigin;
use database::collab::CollabStorage;
use database::collab::GetCollabOrigin;
use database_entity::dto::QueryCollab;
use database_entity::dto::QueryCollabParams;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

pub fn cell_data_to_serde(
  cell_data: Cell,
  field: &Field,
  type_option_reader_by_id: &HashMap<String, Box<dyn TypeOptionCellReader>>,
) -> serde_json::Value {
  match type_option_reader_by_id.get(&field.id) {
    Some(tor) => tor.json_cell(&cell_data),
    None => {
      tracing::error!("Failed to get type option reader by id: {}", field.id);
      serde_json::Value::Null
    },
  }
}

pub fn get_row_details_by_id(
  cells: Cells,
  field_by_id_name_uniq: &HashMap<String, Field>,
  type_option_reader_by_id: &HashMap<String, Box<dyn TypeOptionCellReader>>,
) -> HashMap<String, HashMap<String, serde_json::Value>> {
  let mut row_details: HashMap<String, HashMap<String, serde_json::Value>> =
    HashMap::with_capacity(cells.len());

  for (field_id, field) in field_by_id_name_uniq {
    let cell: Cell = match cells.get(field_id) {
      Some(cell) => cell.clone(),
      None => {
        tracing::error!("Failed to get cell by field id: {}", field.id);
        Cell::new()
      },
    };
    let cell_value = cell_data_to_serde(cell, field, type_option_reader_by_id);
    row_details.insert(
      field.name.clone(),
      HashMap::from([(CELL_DATA.to_string(), cell_value)]),
    );
  }

  row_details
}

pub fn selection_name_by_id(fields: &[Field]) -> HashMap<String, String> {
  let mut selection_name_by_id: HashMap<String, String> = HashMap::new();
  for field in fields {
    let field_type = FieldType::from(field.field_type);
    match field_type {
      FieldType::SingleSelect | FieldType::MultiSelect => {
        selection_id_name_pairs(&field.type_options, &field_type)
          .into_iter()
          .for_each(|(id, name)| {
            selection_name_by_id.insert(id, name);
          })
      },
      _ => (),
    }
  }
  selection_name_by_id
}

pub fn selection_id_by_name(fields: &[Field]) -> HashMap<String, String> {
  let mut selection_id_by_name: HashMap<String, String> = HashMap::new();
  for field in fields {
    let field_type = FieldType::from(field.field_type);
    match field_type {
      FieldType::SingleSelect | FieldType::MultiSelect => {
        selection_id_name_pairs(&field.type_options, &field_type)
          .into_iter()
          .for_each(|(id, name)| {
            selection_id_by_name.insert(name, id);
          })
      },
      _ => (),
    }
  }
  selection_id_by_name
}

/// create a map of field name to field
/// if the field name is repeated, it will be appended with the field id,
pub fn field_by_name_uniq(mut fields: Vec<Field>) -> HashMap<String, Field> {
  fields.sort_by_key(|a| a.id.clone());
  let mut uniq_name_set: HashSet<String> = HashSet::with_capacity(fields.len());
  let mut field_by_name: HashMap<String, Field> = HashMap::with_capacity(fields.len());

  for field in fields {
    // if the name already exists, append the field id to the name
    let name = if uniq_name_set.contains(&field.name) {
      format!("{}-{}", field.name, field.id)
    } else {
      field.name.clone()
    };
    uniq_name_set.insert(name.clone());
    field_by_name.insert(name, field);
  }
  field_by_name
}

/// create a map of field id to field name, and ensure that the field name is unique.
/// if the field name is repeated, it will be appended with the field id,
/// under practical usage circumstances, no other collision should occur
pub fn field_by_id_name_uniq(mut fields: Vec<Field>) -> HashMap<String, Field> {
  fields.sort_by_key(|a| a.id.clone());
  let mut uniq_name_set: HashSet<String> = HashSet::with_capacity(fields.len());
  let mut field_by_id: HashMap<String, Field> = HashMap::with_capacity(fields.len());

  for mut field in fields {
    // if the name already exists, append the field id to the name
    if uniq_name_set.contains(&field.name) {
      let new_name = format!("{}-{}", field.name, field.id);
      field.name.clone_from(&new_name);
    }
    uniq_name_set.insert(field.name.clone());
    field_by_id.insert(field.id.clone(), field);
  }
  field_by_id
}

/// create a map type option reader by field id
pub fn type_option_reader_by_id(
  fields: &[Field],
) -> HashMap<String, Box<dyn TypeOptionCellReader>> {
  let mut type_option_reader_by_id: HashMap<String, Box<dyn TypeOptionCellReader>> =
    HashMap::with_capacity(fields.len());
  for field in fields {
    let field_id: String = field.id.clone();
    let type_option_reader: Box<dyn TypeOptionCellReader> = {
      let field_type: &FieldType = &FieldType::from(field.field_type);
      let type_option_data: TypeOptionData = match field.type_options.get(&field_type.type_id()) {
        Some(tod) => tod.clone(),
        None => HashMap::new(),
      };
      type_option_cell_reader(type_option_data, field_type)
    };
    type_option_reader_by_id.insert(field_id, type_option_reader);
  }
  type_option_reader_by_id
}

pub fn type_options_serde(
  type_options: &TypeOptions,
  field_type: &FieldType,
) -> HashMap<String, serde_json::Value> {
  let type_option = match type_options.get(&field_type.type_id()) {
    Some(type_option) => type_option,
    None => return HashMap::new(),
  };

  let mut result = HashMap::with_capacity(type_option.len());
  for (key, value) in type_option {
    match field_type {
      FieldType::SingleSelect | FieldType::MultiSelect | FieldType::Media => {
        if let yrs::Any::String(arc_str) = value {
          if let Ok(serde_value) = serde_json::from_str::<serde_json::Value>(arc_str) {
            result.insert(key.clone(), serde_value);
          }
        }
      },
      _ => {
        result.insert(key.clone(), serde_json::to_value(value).unwrap_or_default());
      },
    }
  }

  result
}

pub fn collab_from_doc_state(doc_state: Vec<u8>, object_id: &str) -> Result<Collab, AppError> {
  let collab = Collab::new_with_source(
    CollabOrigin::Server,
    object_id,
    DataSource::DocStateV1(doc_state),
    vec![],
    false,
  )
  .map_err(|e| AppError::Unhandled(e.to_string()))?;
  Ok(collab)
}

pub async fn get_database_body(
  collab_storage: &CollabAccessControlStorage,
  workspace_uuid_str: &str,
  database_uuid_str: &str,
) -> Result<(Collab, DatabaseBody), AppError> {
  let db_collab = get_latest_collab(
    collab_storage,
    GetCollabOrigin::Server,
    workspace_uuid_str,
    database_uuid_str,
    CollabType::Database,
  )
  .await?;
  let db_body = DatabaseBody::from_collab(
    &db_collab,
    Arc::new(NoPersistenceDatabaseCollabService),
    None,
  )
  .ok_or_else(|| {
    AppError::Internal(anyhow::anyhow!(
      "Failed to create database body from collab, db_collab_id: {}",
      database_uuid_str,
    ))
  })?;
  Ok((db_collab, db_body))
}

pub fn encode_collab_v1_bytes(
  collab: &Collab,
  collab_type: CollabType,
) -> Result<Vec<u8>, AppError> {
  let bs = collab
    .encode_collab_v1(|collab| collab_type.validate_require_data(collab))
    .map_err(|e| AppError::Unhandled(e.to_string()))?
    .encode_to_bytes()?;
  Ok(bs)
}

pub async fn get_latest_collab_encoded(
  collab_storage: &CollabAccessControlStorage,
  collab_origin: GetCollabOrigin,
  workspace_id: &str,
  oid: &str,
  collab_type: CollabType,
) -> Result<EncodedCollab, AppError> {
  collab_storage
    .get_encode_collab(
      collab_origin,
      QueryCollabParams {
        workspace_id: workspace_id.to_string(),
        inner: QueryCollab {
          object_id: oid.to_string(),
          collab_type,
        },
      },
      true,
    )
    .await
}

pub async fn get_latest_collab(
  storage: &CollabAccessControlStorage,
  origin: GetCollabOrigin,
  workspace_id: &str,
  oid: &str,
  collab_type: CollabType,
) -> Result<Collab, AppError> {
  let ec = get_latest_collab_encoded(storage, origin, workspace_id, oid, collab_type).await?;
  let collab: Collab = Collab::new_with_source(CollabOrigin::Server, oid, ec.into(), vec![], false)
    .map_err(|e| {
      AppError::Internal(anyhow::anyhow!(
        "Failed to create collab from encoded collab: {:?}",
        e
      ))
    })?;
  Ok(collab)
}

pub fn new_cell_from_value(cell_value: serde_json::Value, field: &Field) -> Option<Cell> {
  let field_type = FieldType::from(field.field_type);
  let cell_value: Option<yrs::any::Any> = match field_type {
    FieldType::Relation | FieldType::Media => {
      if let serde_json::Value::Array(arr) = cell_value {
        let mut acc = Vec::with_capacity(arr.len());
        for v in arr {
          if let serde_json::Value::String(value_str) = v {
            acc.push(yrs::any::Any::String(value_str.into()));
          }
        }
        Some(yrs::any::Any::Array(acc.into()))
      } else {
        tracing::warn!("invalid media/relation value: {:?}", cell_value);
        None
      }
    },
    FieldType::RichText | FieldType::URL | FieldType::Summary | FieldType::Translate => {
      if let serde_json::Value::String(value_str) = cell_value {
        Some(yrs::any::Any::String(value_str.into()))
      } else {
        Some(yrs::any::Any::String(cell_value.to_string().into()))
      }
    },
    FieldType::Checkbox => {
      let is_yes = match cell_value {
        serde_json::Value::Null => false,
        serde_json::Value::Bool(b) => b,
        serde_json::Value::Number(n) => n.is_i64() && n.as_i64().unwrap() >= 1,
        serde_json::Value::String(s) => s.to_lowercase() == "yes",
        _ => {
          tracing::warn!("invalid checklist value: {:?}", cell_value);
          false
        },
      };
      if is_yes {
        Some(yrs::any::Any::String("Yes".into()))
      } else {
        None
      }
    },
    FieldType::Number => match cell_value {
      serde_json::Value::Number(n) => Some(yrs::any::Any::String(n.to_string().into())),
      serde_json::Value::String(s) => Some(yrs::any::Any::String(s.into())),
      _ => {
        tracing::warn!("invalid number value: {:?}", cell_value);
        None
      },
    },
    FieldType::SingleSelect => match cell_value {
      serde_json::Value::String(s) => {
        let selection_name_by_id = selection_name_by_id(std::slice::from_ref(field));
        match selection_name_by_id.get(&s) {
          Some(_name) => Some(yrs::any::Any::String(s.into())),
          None => {
            let selection_id_by_name = selection_id_by_name(std::slice::from_ref(field));
            match selection_id_by_name.get(&s) {
              Some(id) => Some(yrs::any::Any::String(id.as_str().into())),
              None => {
                tracing::warn!("invalid single select value for field: {:?}", field.name);
                None
              },
            }
          },
        }
      },
      _ => {
        tracing::warn!("invalid single value: {:?}", cell_value);
        None
      },
    },
    FieldType::MultiSelect => {
      let selection_name_by_id = selection_name_by_id(std::slice::from_ref(field));
      let selection_id_by_name = selection_id_by_name(std::slice::from_ref(field));
      let input_ids: Vec<&str> = match cell_value {
        serde_json::Value::String(ref s) => s.split(',').collect(),
        serde_json::Value::Array(ref arr) => arr.iter().flat_map(|v| v.as_str()).collect(),
        _ => {
          tracing::warn!("invalid multi select value: {:?}", cell_value);
          vec![]
        },
      };

      let mut sel_ids = Vec::with_capacity(input_ids.len());
      for input_id in input_ids {
        if let Some(_name) = selection_name_by_id.get(input_id) {
          sel_ids.push(input_id.to_owned());
        } else if let Some(id) = selection_id_by_name.get(input_id) {
          sel_ids.push(id.to_owned());
        } else {
          tracing::warn!("invalid multi select value: {:?}", cell_value);
        }
      }
      yrs::any::Any::String(sel_ids.join(",").into()).into()
    },
    FieldType::DateTime => match cell_value {
      serde_json::Value::Number(number) => {
        let int_value = number.as_i64().unwrap_or_default();
        Some(yrs::any::Any::String(int_value.to_string().into()))
      },
      serde_json::Value::String(s) => match s.parse::<i64>() {
        Ok(int_value) => Some(yrs::any::Any::String(int_value.to_string().into())),
        Err(_err) => match chrono::DateTime::parse_from_rfc3339(&s) {
          Ok(dt) => Some(yrs::any::Any::String(dt.timestamp().to_string().into())),
          Err(err) => {
            tracing::warn!("Failed to parse datetime string: {:?}", err);
            None
          },
        },
      },
      _ => {
        tracing::warn!("invalid datetime value: {:?}", cell_value);
        None
      },
    },
    FieldType::Checklist => match serde_json::to_string(&cell_value) {
      Ok(s) => Some(yrs::any::Any::String(s.into())),
      Err(err) => {
        tracing::error!("Failed to serialize cell value: {:?}", err);
        None
      },
    },
    FieldType::LastEditedTime | FieldType::CreatedTime | FieldType::Time => {
      // should not be possible
      tracing::error!(
        "attempt to insert into invalid field: {:?}, value: {}",
        field_type,
        cell_value
      );
      None
    },
  };

  cell_value.map(|v| {
    let mut new_cell = new_cell_builder(field_type);
    new_cell.insert(CELL_DATA.to_string(), v);
    new_cell
  })
}

fn selection_id_name_pairs(
  type_options: &TypeOptions,
  field_type: &FieldType,
) -> Vec<(String, String)> {
  if let Some(type_opt) = type_options.get(&field_type.type_id()) {
    if let Some(yrs::Any::String(arc_str)) = type_opt.get("content") {
      if let Ok(serde_value) = serde_json::from_str::<serde_json::Value>(arc_str) {
        if let Some(selections) = serde_value.get("options").and_then(|v| v.as_array()) {
          let mut acc = Vec::with_capacity(selections.len());
          for selection in selections {
            if let serde_json::Value::Object(selection) = selection {
              if let (Some(id), Some(name)) = (
                selection.get("id").and_then(|v| v.as_str()),
                selection.get("name").and_then(|v| v.as_str()),
              ) {
                acc.push((id.to_owned(), name.to_owned()));
              }
            }
          }

          return acc;
        }
      }
    }
  };
  vec![]
}
