# Diesel Transaction Handles
This crate adds a new type of Connection to diesel that is inherently transactional. It will open a transaction on creation, and will rollback on drop. Committing the transaction will consume `self`.

A prime use case for this is the ability to have multiple threads perform db operations within the same transaction.

Additionally, with the feature `rollback_hooks`, you can provide functions to execute in case the transaction rolls back.
The main use case of this is if you perform IO that is not to your db, that you need to undo in the case of a rollback, you can provide the operation necessary to rollback to the connection and it will be performed if necessary.

Rollback hooks are performed last in, first out.

## Usage
Add this to your `Cargo.toml`:
```toml
[dependencies]
diesel_transaction_handles = "0.1.0"
```

### Example:
```rust
use diesel::prelude::*;
use diesel_transaction_handles::Transaction;
use std::sync::Arc;

fn main() {

    let pgcon = diesel::PgConnection::establish("localhost:5432/pgdb").unwrap();
    let txcon = TransactionalConnection::new(pgcon).unwrap();
    let arccon = Arc::new(txcon);

    let arccon_ = arccon.clone();
    let job1 = std::thread::spawn(move || {
        println!("SELECTing TRUE");
        arccon_.add_rollback_hook(|| Ok(println!("Nevermind, rolling back.")));
        diesel::select(diesel::dsl::sql::<diesel::sql_types::Bool>("TRUE")).load::<bool>(&*arccon_)
    });

    let arccon_ = arccon.clone();
    let job2 = std::thread::spawn(move || {
        diesel::select(diesel::dsl::sql::<diesel::sql_types::Bool>("FALSE")).load::<bool>(&*arccon_)
    });

    let job1res = job1.join().unwrap();
    let job2res = job2.join().unwrap();
    let res = match (job1res, job2res) {
        (Ok(a), Ok(b)) => Ok((a, b)),
        (Err(e), _) => Err(e),
        (_, Err(e)) => Err(e),
    };
    println!(
        "{:?}",
        Arc::try_unwrap(arccon)
            .map_err(|_| "Arc still held by multiple threads.")
            .unwrap()
            .handle_result(res)
    );
}
```
