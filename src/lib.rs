#[macro_use]
extern crate failure;

use diesel::connection::Connection;
use diesel::connection::SimpleConnection;
use diesel::connection::TransactionManager;
use diesel::result::Error as DieselError;
use failure::Error as FailureError;
use std::sync::Mutex;

#[cfg(all(feature = "log_errors_on_drop", feature = "panic_errors_on_drop"))]
compile_error!(
    "Features: \"log_errors_on_drop\" and \"panic_errors_on_drop\" are mutually exclusive!"
);

#[cfg(all(nightly, feature = "rollback_hooks"))]
pub type RollbackHook = Box<dyn FnOnce() -> Result<(), failure::Error> + Send>;
#[cfg(all(not(nightly), feature = "rollback_hooks"))]
pub type RollbackHook = Box<fn() -> Result<(), failure::Error>>;

#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "Diesel Error: {}", _0)]
    Diesel(DieselError),
    #[fail(display = "Custom Error: {}", _0)]
    Failure(FailureError),
}
impl From<DieselError> for Error {
    fn from(err: DieselError) -> Self {
        Error::Diesel(err)
    }
}
impl From<FailureError> for Error {
    fn from(err: FailureError) -> Self {
        Error::Failure(err)
    }
}

#[derive(Debug, Fail)]
#[fail(display = "The following errors ocurred: {:?}.", _0)]
pub struct ErrorVec(pub Vec<Error>);
impl From<Vec<Error>> for ErrorVec {
    fn from(err_vec: Vec<Error>) -> Self {
        ErrorVec(err_vec)
    }
}

#[cfg(feature = "rollback_hooks")]
pub struct TransactionalConnection<T: Connection>(Mutex<T>, Mutex<Vec<RollbackHook>>);
#[cfg(not(feature = "rollback_hooks"))]
pub struct TransactionalConnection<T: Connection>(Mutex<T>);
impl<C: Connection> TransactionalConnection<C> {
    pub fn new(conn: C) -> Result<TransactionalConnection<C>, Error> {
        let man = conn.transaction_manager();

        man.begin_transaction(&conn)?;

        #[cfg(feature = "rollback_hooks")]
        let res = Ok(TransactionalConnection(
            Mutex::new(conn),
            Mutex::new(vec![]),
        ));
        #[cfg(not(feature = "rollback_hooks"))]
        let res = Ok(TransactionalConnection(Mutex::new(conn)));

        res
    }

    pub fn rollback(self) -> Result<(), ErrorVec> {
        let mut errs = vec![];
        #[cfg(feature = "rollback_hooks")]
        {
            let mut guard = self.1.lock().unwrap();
            while !guard.is_empty() {
                let hook = guard.pop().unwrap();
                match hook() {
                    Err(e) => errs.push(Error::from(e)),
                    _ => (),
                }
            }
        }

        let guard = self.0.lock().unwrap();
        let man = guard.transaction_manager();

        while TransactionManager::<C>::get_transaction_depth(man) > 0 {
            match man.rollback_transaction(&*guard) {
                Err(e) => {
                    errs.push(Error::from(e));
                    break;
                }
                _ => (),
            }
        }

        if !errs.is_empty() {
            Err(errs)?;
        }

        Ok(())
    }

    pub fn commit(self) -> Result<(), Error> {
        let guard = self.0.lock().unwrap();
        let man = guard.transaction_manager();

        while TransactionManager::<C>::get_transaction_depth(man) > 0 {
            man.commit_transaction(&*guard)?;
        }

        Ok(())
    }

    pub fn handle_result<T, E>(self, res: Result<T, E>) -> Result<T, failure::Error>
    where
        failure::Error: From<E>,
    {
        match &res {
            Ok(_) => self.commit()?,
            Err(_) => self.rollback()?,
        };
        Ok(res?)
    }

    #[cfg(all(nightly, feature = "rollback_hooks"))]
    pub fn add_rollback_hook(&self, hook: impl FnOnce() -> Result<(), failure::Error> + Send) {
        self.1.lock().unwrap().push(Box::new(hook));
    }

    #[cfg(all(not(nightly), feature = "rollback_hooks"))]
    pub fn add_rollback_hook(&self, hook: fn() -> Result<(), failure::Error>) {
        self.1.lock().unwrap().push(Box::new(hook));
    }
}

impl<C: Connection> std::ops::Drop for TransactionalConnection<C> {
    fn drop(&mut self) {
        #[cfg(feature = "rollback_hooks")]
        {
            let guard = self.1.lock().unwrap();
            for hook in &*guard {
                let _res = hook();

                #[cfg(feature = "log_errors_on_drop")]
                _res.unwrap_or_else(|e| {
                    eprintln!(
                        "WARNING: Error ocurred while attempting transaction rollback: {}",
                        e
                    );
                });

                #[cfg(feature = "panic_errors_on_drop")]
                _res.unwrap_or_else(|e| panic!("{}", e));
            }
        }

        let guard = self.0.lock().unwrap();
        let man = guard.transaction_manager();

        while TransactionManager::<C>::get_transaction_depth(man) > 0 {
            let _res = man.rollback_transaction(&*guard);

            #[cfg(feature = "log_errors_on_drop")]
            _res.unwrap_or_else(|e| {
                eprintln!(
                    "WARNING: Error ocurred while attempting transaction rollback: {}",
                    e
                );
            });

            #[cfg(feature = "panic_errors_on_drop")]
            _res.unwrap_or_else(|e| panic!("{}", e));
        }
    }
}

impl<C: Connection> SimpleConnection for TransactionalConnection<C> {
    fn batch_execute(&self, query: &str) -> diesel::QueryResult<()> {
        self.0.lock().unwrap().batch_execute(query)
    }
}

impl<C: Connection> Connection for TransactionalConnection<C> {
    type Backend = C::Backend;
    type TransactionManager = Self;

    fn establish(_: &str) -> diesel::ConnectionResult<Self> {
        Err(diesel::ConnectionError::BadConnection(String::from(
            "Cannot directly establish a pooled connection",
        )))
    }

    fn execute(&self, query: &str) -> diesel::QueryResult<usize> {
        self.0.lock().unwrap().execute(query)
    }

    fn query_by_index<T, U>(&self, source: T) -> diesel::QueryResult<Vec<U>>
    where
        T: diesel::query_builder::AsQuery,
        T::Query:
            diesel::query_builder::QueryFragment<Self::Backend> + diesel::query_builder::QueryId,
        Self::Backend: diesel::sql_types::HasSqlType<T::SqlType>,
        U: diesel::deserialize::Queryable<T::SqlType, Self::Backend>,
    {
        self.0.lock().unwrap().query_by_index(source)
    }

    fn query_by_name<T, U>(&self, source: &T) -> diesel::QueryResult<Vec<U>>
    where
        T: diesel::query_builder::QueryFragment<Self::Backend> + diesel::query_builder::QueryId,
        U: diesel::deserialize::QueryableByName<Self::Backend>,
    {
        self.0.lock().unwrap().query_by_name(source)
    }

    fn execute_returning_count<T>(&self, source: &T) -> diesel::QueryResult<usize>
    where
        T: diesel::query_builder::QueryFragment<Self::Backend> + diesel::query_builder::QueryId,
    {
        self.0.lock().unwrap().execute_returning_count(source)
    }

    fn transaction_manager(&self) -> &Self::TransactionManager {
        &self
    }
}

impl<C: Connection> TransactionManager<TransactionalConnection<C>> for TransactionalConnection<C> {
    fn begin_transaction(&self, conn: &TransactionalConnection<C>) -> diesel::QueryResult<()> {
        let conn = conn.0.lock().unwrap();
        conn.transaction_manager().begin_transaction(&*conn)
    }

    fn rollback_transaction(&self, conn: &TransactionalConnection<C>) -> diesel::QueryResult<()> {
        let conn = conn.0.lock().unwrap();
        conn.transaction_manager().rollback_transaction(&*conn)
    }

    fn commit_transaction(&self, conn: &TransactionalConnection<C>) -> diesel::QueryResult<()> {
        let conn = conn.0.lock().unwrap();
        conn.transaction_manager().commit_transaction(&*conn)
    }

    fn get_transaction_depth(&self) -> u32 {
        diesel::connection::TransactionManager::<C>::get_transaction_depth(
            &*self.0.lock().unwrap().transaction_manager(),
        )
    }
}

#[test]
fn multiple_threads() {
    use diesel::prelude::*;
    use std::sync::Arc;

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
