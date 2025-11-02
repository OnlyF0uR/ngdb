//! Procedural macros for NGDB
//!
//! This crate provides attribute macros to reduce boilerplate when working with NGDB.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields, Lit, Type};

/// Attribute macro for defining an NGDB collection
///
/// This macro automatically:
/// - Adds `BorshSerialize`, `BorshDeserialize`, `Clone`, and `Debug` derives
/// - Generates `collection_name()` method that returns the collection name
/// - Generates `collection(&Database)` method that returns the typed collection
/// - Generates `save(&self, db: &Database)` method for saving to the database
/// - Implements `Referable` trait with automatic `resolve_all()`
///
/// # Usage
///
/// ```ignore
/// use ngdb::Ref;
///
/// #[ngdb("users")]
/// struct User {
///     id: u64,
///     name: String,
/// }
///
/// impl Storable for User {
///     type Key = u64;
///     fn key(&self) -> Self::Key { self.id }
/// }
///
/// #[ngdb("posts")]
/// struct Post {
///     id: u64,
///     title: String,
///     author: Ref<User>,  // automatically resolved!
/// }
///
/// impl Storable for Post {
///     type Key = u64;
///     fn key(&self) -> Self::Key { self.id }
/// }
///
/// // Now you can use:
/// post.save(&db)?;                    // Save to database
/// Post::collection_name()             // Returns "posts"
/// let posts = Post::collection(&db)?; // Get typed collection
/// // resolve_all() is automatically generated to resolve the author reference
/// ```
#[proc_macro_attribute]
pub fn ngdb(attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut input = parse_macro_input!(item as DeriveInput);

    // Parse the collection name from the attribute
    let collection_name = if attr.is_empty() {
        panic!("ngdb attribute requires a collection name: #[ngdb(\"name\")]");
    } else {
        let lit = parse_macro_input!(attr as Lit);
        match lit {
            Lit::Str(s) => s.value(),
            _ => panic!("ngdb attribute value must be a string literal"),
        }
    };

    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    // Find all Ref<T> fields
    let ref_fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => fields
                .named
                .iter()
                .filter_map(|f| {
                    let field_name = f.ident.as_ref()?;
                    if let Type::Path(type_path) = &f.ty {
                        let last_segment = type_path.path.segments.last()?;
                        if last_segment.ident == "Ref" {
                            // Extract the T from Ref<T>
                            if let syn::PathArguments::AngleBracketed(args) =
                                &last_segment.arguments
                            {
                                if let Some(syn::GenericArgument::Type(Type::Path(inner_type))) =
                                    args.args.first()
                                {
                                    return Some((field_name.clone(), inner_type.clone()));
                                }
                            }
                        }
                    }
                    None
                })
                .collect::<Vec<_>>(),
            _ => vec![],
        },
        _ => vec![],
    };

    // Add BorshSerialize, BorshDeserialize, Clone, and Debug derives
    // Clone is always added because this type might be referenced by others via Ref<T>
    let derives: Vec<syn::Path> = vec![
        syn::parse_quote!(::borsh::BorshSerialize),
        syn::parse_quote!(::borsh::BorshDeserialize),
        syn::parse_quote!(Clone),
        syn::parse_quote!(Debug),
    ];

    // Create or extend the derive attribute
    let derive_attr: syn::Attribute = syn::parse_quote! {
        #[derive(#(#derives),*)]
    };

    // Add the derive attribute to the struct
    input.attrs.push(derive_attr);

    // Generate resolve statements for each Ref<T> field
    // Also resolve nested references in the resolved objects
    let resolve_statements = ref_fields.iter().map(|(field_name, inner_type)| {
        quote! {
            self.#field_name.resolve(db, <#inner_type>::collection_name())?;
            if let Ok(resolved) = self.#field_name.get_mut() {
                resolved.resolve_all(db)?;
            }
        }
    });

    let expanded = quote! {
        #input

        impl #impl_generics #name #ty_generics #where_clause {
            /// Returns the collection name for this type
            pub fn collection_name() -> &'static str {
                #collection_name
            }

            /// Get the typed collection for this type from the database
            pub fn collection(db: &::ngdb::Database) -> ::ngdb::Result<::ngdb::Collection<Self>>
            where
                Self: ::ngdb::Storable,
            {
                db.collection::<Self>(#collection_name)
            }

            /// Save this object to the database
            ///
            /// This is a convenience method that gets the collection and calls put().
            pub fn save(&self, db: &::ngdb::Database) -> ::ngdb::Result<()>
            where
                Self: ::ngdb::Storable,
            {
                let collection = Self::collection(db)?;
                collection.put(self)
            }
        }

        impl #impl_generics ::ngdb::Referable for #name #ty_generics #where_clause {
            fn resolve_all(&mut self, db: &::ngdb::Database) -> ::ngdb::Result<()> {
                #(#resolve_statements)*
                Ok(())
            }
        }
    };

    TokenStream::from(expanded)
}
