//! Basic usage example for NGDB
//!
//! Demonstrates core CRUD operations with a simple product database.

use ngdb::{DatabaseConfig, Result, Storable};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Product {
    id: u64,
    name: String,
    price: f64,
    stock: u32,
}

impl Storable for Product {
    type Key = u64;

    fn key(&self) -> Self::Key {
        self.id
    }

    // Optional: validate data before storing
    fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(ngdb::Error::InvalidValue(
                "Product name cannot be empty".to_string(),
            ));
        }
        if self.price < 0.0 {
            return Err(ngdb::Error::InvalidValue(
                "Product price cannot be negative".to_string(),
            ));
        }
        Ok(())
    }
}

fn main() -> Result<()> {
    // Open database with a column family for products
    let db = DatabaseConfig::new("./data/products")
        .create_if_missing(true)
        .add_column_family("products")
        .open()?;

    let products = db.collection::<Product>("products")?;

    // Create and store products
    let laptop = Product {
        id: 1,
        name: "Laptop".to_string(),
        price: 999.99,
        stock: 15,
    };
    products.put(&laptop)?;

    let mouse = Product {
        id: 2,
        name: "Mouse".to_string(),
        price: 29.99,
        stock: 100,
    };
    products.put(&mouse)?;

    // Retrieve a single item
    if let Some(product) = products.get(&1)? {
        println!("Found: {} - ${}", product.name, product.price);
    }

    // Check existence
    if products.exists(&2)? {
        println!("Product 2 exists");
    }

    // Multi-get for efficient batch retrieval
    let ids = vec![1, 2];
    let results = products.get_many(&ids)?;
    println!("Retrieved {} products via multi-get", results.len());

    // Iterate over all items
    println!("All products:");
    products.iter().for_each(|p| {
        println!("  {}: ${}", p.name, p.price);
        true
    })?;

    // Update an item
    let updated = Product {
        id: 1,
        name: "Gaming Laptop".to_string(),
        price: 1499.99,
        stock: 10,
    };
    products.put(&updated)?;

    // Delete an item
    products.delete(&2)?;
    println!("Deleted product 2");

    // Get statistics
    let count = products.iter().count()?;
    println!("Total products: {}", count);

    // Flush and shutdown
    products.flush()?;
    db.shutdown()?;

    Ok(())
}
