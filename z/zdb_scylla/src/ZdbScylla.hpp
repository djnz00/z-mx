// FIXME - adopt ZmHeap
//
// note that back-end must maintain "_mrd" table: (most recent deletes)
//   ZuID DB	// PK
//   UN un	// update number of most recent delete
//   SN sn	// sequence number of most recent delete
//
// _mrd is updated together with the delete in a batch, to ensure EC:
//
// cass_batch_new
// cass_batch_add_statement(...)
// cass_session_execute_batch(...)
// cass_batch_free
//
// check:
// cass_batch_set_consistency // generally should be LOCAL_ONE for writes, LOCAL_SERIAL for reads
// cass_batch_set_is_idempotent // use this, set to true
//
//
  // importer {row, indices[]} -> value_get_*(row_get_column(), ...)
  // exporter {statement, indices[]} -> statement_bind_*()
  //
  // note that 
  //
  // struct Exporter : public ZtField::Exporter {
  //   unsigned		indices[];	// MField index -> statement index
  //   // probably have prepared statement in here as well
  // };
  // struct Export : public ZtField::Export {
  //   const auto &exporter_() {
  //     return static_cast<Exporter &>(exporter);
  //   }
  //   CassStatement	*statement;
  // }
  //
  // // same for Importer, except with row instead of statement
  //
  // 1 importer (get)
  // 3 exporters (push, update, del)

#if 0
template <typename L>
void cass_future_set_lambda(CassFuture *future, L l) {
  using Context_ = Context<L>;
  Context_ *context = new Context_{ZuMv(l)};
  cass_future_set_callback(future, Context_::invoke, context)
}

typedef void (*CassFutureCallback)(CassFuture* future, void* data);
cass_future_set_callback(CassFuture *future, CassFutureCallback fn, data)
#endif
