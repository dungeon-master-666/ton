#include "emulator-extern.h"
#include "td/utils/base64.h"
#include "td/utils/Status.h"
#include "td/utils/JsonBuilder.h"
#include "td/utils/logging.h"
#include "td/utils/Variant.h"
#include "td/utils/overloaded.h"
#include "transaction-emulator.h"
#include "tvm-emulator.hpp"
#include "crypto/vm/stack.hpp"

td::Result<vm::StackEntry> from_emulator_api(td::JsonValue& entry) {
  if (entry.type() != td::JsonValue::Type::Object) {
    return td::Status::Error(PSLICE() << "Stack entry of object type expected");
  }
  auto& object = entry.get_object();
  TRY_RESULT(type, td::get_json_object_string_field(object, "type", false));

  if (type == "cell") {
    TRY_RESULT(value, td::get_json_object_field(object, "value", td::JsonValue::Type::String, false));
    auto boc_b64 = value.get_string();
    TRY_RESULT(boc_decoded, td::base64_decode(td::Slice(boc_b64)));
    TRY_RESULT(cell, vm::std_boc_deserialize(std::move(boc_decoded)));
    return vm::StackEntry(std::move(cell));
  }
  if (type == "cell_slice") {
    TRY_RESULT(value, td::get_json_object_field(object, "value", td::JsonValue::Type::String, false));
    auto boc_b64 = value.get_string();
    TRY_RESULT(boc_decoded, td::base64_decode(td::Slice(boc_b64)));
    TRY_RESULT(cell, vm::std_boc_deserialize(std::move(boc_decoded)));
    auto slice = vm::load_cell_slice_ref(std::move(cell));
    return vm::StackEntry(slice);
  }
  if (type == "number") {
    TRY_RESULT(value, td::get_json_object_field(object, "value", td::JsonValue::Type::String, false));
    auto num = td::dec_string_to_int256(value.get_string());
    if (num.is_null()) {
      return td::Status::Error("Error parsing string to int256");
    } 
    return vm::StackEntry(num);
  }
  if (type == "tuple") {
    std::vector<vm::StackEntry> elements;
    TRY_RESULT(value, td::get_json_object_field(object, "value", td::JsonValue::Type::Array, false));
    for (auto& element : value.get_array()) {
      TRY_RESULT(new_element, from_emulator_api(element));
      elements.push_back(std::move(new_element));
    }
    return td::Ref<vm::Tuple>(true, std::move(elements));
  }
  if (type == "null") {
    return vm::StackEntry();
  }
  
  return td::Status::Error(PSLICE() << "Unsupported type: " << type);
}

class StackEntryJsonable: public td::Jsonable {
  vm::StackEntry entry_;
public:
  explicit StackEntryJsonable(const vm::StackEntry &entry) : entry_(entry) {
  }

  void store(td::JsonValueScope *scope) const {
    std::string type;
    td::Variant<td::JsonValue, td::JsonArray> value;
    switch (entry_.type()) {
      case vm::StackEntry::t_cell: {
        auto boc = vm::std_boc_serialize(entry_.as_cell(), vm::BagOfCells::Mode::WithCRC32C);
        CHECK(boc.is_ok());
        auto boc_b64 = td::base64_encode(boc.move_as_ok().as_slice());
        auto object = scope->enter_object();
        object("type", "cell");
        object("value", std::move(boc_b64));
        break;
      }
      case vm::StackEntry::t_slice: {
        auto cell = vm::CellBuilder().append_cellslice(entry_.as_slice()).finalize();
        auto boc = vm::std_boc_serialize(std::move(cell), vm::BagOfCells::Mode::WithCRC32C);
        auto boc_b64 = td::base64_encode(boc.move_as_ok().as_slice());
        auto object = scope->enter_object();
        object("type", "cell_slice");
        object("value", std::move(boc_b64));
        break;
      }
      case vm::StackEntry::t_int: {
        auto object = scope->enter_object();
        object("type", "number");
        object("value", dec_string(entry_.as_int()));
        break;
      }
      case vm::StackEntry::t_tuple: {
        auto object = scope->enter_object();
        td::JsonBuilder jb;
        auto array = jb.enter_array();
        for (const auto& x : *entry_.as_tuple()) {
          array << StackEntryJsonable(x);
        }
        array.leave();
        object("type", "tuple");
        object("value", td::JsonRaw(jb.string_builder().as_cslice()));
        break;
      }
      case vm::StackEntry::t_null: {
        auto object = scope->enter_object();
        object("type", "null");
        object("value", td::JsonNull());
        break;
      }
      default: {
        auto object = scope->enter_object();
        object("type", "UNSUPPORTED STACK ENTRY TYPE");
        object("value", td::JsonNull());
        break;
      }
    }
  }
};

class StackJsonable: public td::Jsonable {
  td::Ref<vm::Stack> stack_;
public:
  explicit StackJsonable(td::Ref<vm::Stack> stack) : stack_(stack) {  
  }

  void store(td::JsonValueScope *scope) const {
    auto array = scope->enter_array();
    for (auto& entry : stack_->as_span()) {
      array << StackEntryJsonable(entry);
    }
  }
};

const char *success_response(std::string&& transaction, std::string&& new_shard_account, std::string&& vm_log) {
  td::JsonBuilder jb;
  auto json_obj = jb.enter_object();
  json_obj("success", td::JsonTrue());
  json_obj("transaction", std::move(transaction));
  json_obj("shard_account", std::move(new_shard_account));
  json_obj("vm_log", std::move(vm_log));
  json_obj.leave();
  return strdup(jb.string_builder().as_cslice().c_str());
}

const char *error_response(std::string&& error) {
  td::JsonBuilder jb;
  auto json_obj = jb.enter_object();
  json_obj("success", td::JsonFalse());
  json_obj("error", std::move(error));
  json_obj.leave();
  return strdup(jb.string_builder().as_cslice().c_str());
}

const char *external_not_accepted_response(std::string&& vm_log, int vm_exit_code) {
  td::JsonBuilder jb;
  auto json_obj = jb.enter_object();
  json_obj("success", td::JsonFalse());
  json_obj("error", "External message not accepted by smart contract");
  json_obj("vm_log", std::move(vm_log));
  json_obj("vm_exit_code", vm_exit_code);
  json_obj.leave();
  return strdup(jb.string_builder().as_cslice().c_str());
}

#define ERROR_RESPONSE(error) return error_response(error)

td::Result<block::Config> decode_config(const char* config_boc) {
  auto config_params_decoded = td::base64_decode(td::Slice(config_boc));
  if (config_params_decoded.is_error()) {
    return config_params_decoded.move_as_error_prefix("Can't decode base64 config params boc: ");
  }
  auto config_params_cell = vm::std_boc_deserialize(config_params_decoded.move_as_ok());
  if (config_params_cell.is_error()) {
    return config_params_cell.move_as_error_prefix("Can't deserialize config params boc: ");
  }
  auto global_config = block::Config(config_params_cell.move_as_ok(), td::Bits256::zero(), block::Config::needWorkchainInfo | block::Config::needSpecialSmc);
  auto unpack_res = global_config.unpack();
  if (unpack_res.is_error()) {
    return unpack_res.move_as_error_prefix("Can't unpack config params: ");
  }
  return global_config;
}

void *transaction_emulator_create(const char *config_params_boc, int vm_log_verbosity) {
  auto global_config_res = decode_config(config_params_boc);
  if (global_config_res.is_error()) {
    LOG(ERROR) << global_config_res.move_as_error().message();
    return nullptr;
  }

  return new emulator::TransactionEmulator(global_config_res.move_as_ok(), vm_log_verbosity);
}

const char *transaction_emulator_emulate_transaction(void *transaction_emulator, const char *shard_account_boc, const char *message_boc, const char *other_params) {
  auto emulator = static_cast<emulator::TransactionEmulator *>(transaction_emulator);
  
  auto message_decoded = td::base64_decode(td::Slice(message_boc));
  if (message_decoded.is_error()) {
    ERROR_RESPONSE(PSTRING() << "Can't decode base64 message boc: " << message_decoded.move_as_error());
  }
  auto message_cell_r = vm::std_boc_deserialize(message_decoded.move_as_ok());
  if (message_cell_r.is_error()) {
    ERROR_RESPONSE(PSTRING() << "Can't deserialize message boc: " << message_cell_r.move_as_error());
  }
  auto message_cell = message_cell_r.move_as_ok();
  auto message_cs = vm::load_cell_slice(message_cell);
  int msg_tag = block::gen::t_CommonMsgInfo.get_tag(message_cs);

  auto shard_account_decoded = td::base64_decode(td::Slice(shard_account_boc));
  if (shard_account_decoded.is_error()) {
    ERROR_RESPONSE(PSTRING() << "Can't decode base64 shard account boc: " << shard_account_decoded.move_as_error());
  }
  auto shard_account_cell = vm::std_boc_deserialize(shard_account_decoded.move_as_ok());
  if (shard_account_cell.is_error()) {
    ERROR_RESPONSE(PSTRING() << "Can't deserialize shard account boc: " << shard_account_cell.move_as_error());
  }
  auto shard_account_slice = vm::load_cell_slice(shard_account_cell.ok_ref());
  block::gen::ShardAccount::Record shard_account;
  if (!tlb::unpack(shard_account_slice, shard_account)) {
    ERROR_RESPONSE(PSTRING() << "Can't unpack shard account cell");
  }

  td::Ref<vm::CellSlice> addr_slice;
  auto account_slice = vm::load_cell_slice(shard_account.account);
  if (block::gen::t_Account.get_tag(account_slice) == block::gen::Account::account_none) {
    if (msg_tag == block::gen::CommonMsgInfo::ext_in_msg_info) {
      block::gen::CommonMsgInfo::Record_ext_in_msg_info info;
      if (!tlb::unpack(message_cs, info)) {
        ERROR_RESPONSE(PSTRING() <<  "Can't unpack inbound external message");
      }
      addr_slice = std::move(info.dest);
    }
    else if (msg_tag == block::gen::CommonMsgInfo::int_msg_info) {
      block::gen::CommonMsgInfo::Record_int_msg_info info;
      if (!tlb::unpack(message_cs, info)) {
          ERROR_RESPONSE(PSTRING() << "Can't unpack inbound internal message");
      }
      addr_slice = std::move(info.dest);
    } else {
      ERROR_RESPONSE(PSTRING() << "Only ext in and int message are supported");
    }
  } else {
    block::gen::Account::Record_account account_record;
    if (!tlb::unpack(account_slice, account_record)) {
      ERROR_RESPONSE(PSTRING() << "Can't unpack account cell");
    }
    addr_slice = std::move(account_record.addr);
  }
  ton::WorkchainId wc;
  ton::StdSmcAddress addr;
  if (!block::tlb::t_MsgAddressInt.extract_std_address(addr_slice, wc, addr)) {
    ERROR_RESPONSE(PSTRING() << "Can't extract account address");
  }

  auto account = block::Account(wc, addr.bits());
  ton::UnixTime now = (unsigned)std::time(nullptr);
  bool is_special = wc == ton::masterchainId && emulator->get_config().is_special_smartcontract(addr);
  if (!account.unpack(vm::load_cell_slice_ref(shard_account_cell.move_as_ok()), td::Ref<vm::CellSlice>(), now, is_special)) {
    ERROR_RESPONSE(PSTRING() << "Can't unpack shard account");
  }

  auto result = emulator->emulate_transaction(std::move(account), message_cell, 0, 0, block::transaction::Transaction::tr_ord);
  if (result.is_error()) {
    ERROR_RESPONSE(PSTRING() << "Emulate transaction failed: " << result.move_as_error());
  }
  auto emulation_result = result.move_as_ok();

  auto external_not_accepted = dynamic_cast<emulator::TransactionEmulator::EmulationExternalNotAccepted *>(emulation_result.get());
  if (external_not_accepted) {
    return external_not_accepted_response(std::move(external_not_accepted->vm_log), external_not_accepted->vm_exit_code);
  }

  auto emulation_success = dynamic_cast<emulator::TransactionEmulator::EmulationSuccess&>(*emulation_result);
  auto trans_boc = vm::std_boc_serialize(std::move(emulation_success.transaction), vm::BagOfCells::Mode::WithCRC32C);
  if (trans_boc.is_error()) {
    ERROR_RESPONSE(PSTRING() << "Can't serialize Transaction to boc" << trans_boc.move_as_error());
  }
  auto trans_boc_b64 = td::base64_encode(trans_boc.move_as_ok().as_slice());

  auto new_shard_account_cell = vm::CellBuilder().store_ref(emulation_success.account.total_state)
                               .store_bits(emulation_success.account.last_trans_hash_.as_bitslice())
                               .store_long(emulation_success.account.last_trans_lt_).finalize();
  auto new_shard_account_boc = vm::std_boc_serialize(std::move(new_shard_account_cell), vm::BagOfCells::Mode::WithCRC32C);
  if (new_shard_account_boc.is_error()) {
    ERROR_RESPONSE(PSTRING() << "Can't serialize ShardAccount to boc" << new_shard_account_boc.move_as_error());
  }
  auto new_shard_account_boc_b64 = td::base64_encode(new_shard_account_boc.move_as_ok().as_slice());

  return success_response(std::move(trans_boc_b64), std::move(new_shard_account_boc_b64), std::move(emulation_success.vm_log));
}

bool transaction_emulator_set_unixtime(void *transaction_emulator, uint32_t unixtime) {
  auto emulator = static_cast<emulator::TransactionEmulator *>(transaction_emulator);

  emulator->set_unixtime(unixtime);

  return true;
}

bool transaction_emulator_set_lt(void *transaction_emulator, uint64_t lt) {
  auto emulator = static_cast<emulator::TransactionEmulator *>(transaction_emulator);

  emulator->set_lt(lt);

  return true;
}

bool transaction_emulator_set_rand_seed(void *transaction_emulator, const char* rand_seed_hex) {
  auto emulator = static_cast<emulator::TransactionEmulator *>(transaction_emulator);

  auto rand_seed_hex_slice = td::Slice(rand_seed_hex);
  if (rand_seed_hex_slice.size() != 64) {
    LOG(ERROR) << "Rand seed expected as 64 characters hex string";
    return false;
  }
  auto rand_seed_bytes = td::hex_decode(rand_seed_hex_slice);
  if (rand_seed_bytes.is_error()) {
    LOG(ERROR) << "Can't decode hex rand seed";
    return false;
  }
  td::BitArray<256> rand_seed;
  rand_seed.as_slice().copy_from(rand_seed_bytes.move_as_ok());

  emulator->set_rand_seed(rand_seed);
  return true;
}

bool transaction_emulator_set_ignore_chksig(void *transaction_emulator, bool ignore_chksig) {
  auto emulator = static_cast<emulator::TransactionEmulator *>(transaction_emulator);

  emulator->set_ignore_chksig(ignore_chksig);

  return true;
}

bool transaction_emulator_set_config(void *transaction_emulator, const char* config_boc) {
  auto emulator = static_cast<emulator::TransactionEmulator *>(transaction_emulator);

  auto global_config_res = decode_config(config_boc);
  if (global_config_res.is_error()) {
    LOG(ERROR) << global_config_res.move_as_error().message();
    return false;
  }

  emulator->set_config(global_config_res.move_as_ok());

  return true;
}

bool transaction_emulator_set_libs(void *transaction_emulator, const char* shardchain_libs_boc) {
  auto emulator = static_cast<emulator::TransactionEmulator *>(transaction_emulator);

  if (shardchain_libs_boc != nullptr) {
    auto shardchain_libs_decoded = td::base64_decode(td::Slice(shardchain_libs_boc));
    if (shardchain_libs_decoded.is_error()) {
      LOG(ERROR) << "Can't decode base64 shardchain libraries boc: " << shardchain_libs_decoded.move_as_error();
      return false;
    }
    auto shardchain_libs_cell = vm::std_boc_deserialize(shardchain_libs_decoded.move_as_ok());
    if (shardchain_libs_cell.is_error()) {
      LOG(ERROR) << "Can't deserialize shardchain libraries boc: " << shardchain_libs_cell.move_as_error();
      return false;
    }
    emulator->set_libs(vm::Dictionary(shardchain_libs_cell.move_as_ok(), 256));
  }

  return true;
}

void transaction_emulator_destroy(void *transaction_emulator) {
  delete static_cast<emulator::TransactionEmulator *>(transaction_emulator);
}

bool emulator_set_verbosity_level(int verbosity_level) {
  if (0 <= verbosity_level && verbosity_level <= VERBOSITY_NAME(NEVER)) {
    SET_VERBOSITY_LEVEL(VERBOSITY_NAME(FATAL) + verbosity_level);
    return true;
  }
  return false;
}

void *tvm_emulator_create(const char *code, const char *data, int vm_log_verbosity) {
  auto code_decoded = td::base64_decode(td::Slice(code));
  if (code_decoded.is_error()) {
    LOG(ERROR) << "Can't deserialize code boc: " << code_decoded.move_as_error();
    return nullptr;
  }
  auto code_cell = vm::std_boc_deserialize(code_decoded.move_as_ok());
  if (code_cell.is_error()) {
    LOG(ERROR) << "Can't deserialize code boc: " << code_cell.move_as_error();
    return nullptr;
  }

  auto data_decoded = td::base64_decode(td::Slice(data));
  if (data_decoded.is_error()) {
    LOG(ERROR) << "Can't deserialize data boc: " << data_decoded.move_as_error();
    return nullptr;
  }
  auto data_cell = vm::std_boc_deserialize(data_decoded.move_as_ok());
  if (data_cell.is_error()) {
    LOG(ERROR) << "Can't deserialize code boc: " << data_cell.move_as_error();
    return nullptr;
  }

  auto emulator = new emulator::TvmEmulator(code_cell.move_as_ok(), data_cell.move_as_ok());
  emulator->set_vm_verbosity_level(vm_log_verbosity);
  return emulator;
}

bool tvm_emulator_set_libraries(void *tvm_emulator, const char *libs_boc) {
  vm::Dictionary libs{256};
  auto libs_decoded = td::base64_decode(td::Slice(libs_boc));
  if (libs_decoded.is_error()) {
    LOG(ERROR) << "Can't decode base64 libraries boc: " << libs_decoded.move_as_error();
    return false;
  }
  auto libs_cell = vm::std_boc_deserialize(libs_decoded.move_as_ok());
  if (libs_cell.is_error()) {
    LOG(ERROR) << "Can't deserialize libraries boc: " << libs_cell.move_as_error();
    return false;
  }
  libs = vm::Dictionary(libs_cell.move_as_ok(), 256);

  auto emulator = static_cast<emulator::TvmEmulator *>(tvm_emulator);
  emulator->set_libraries(std::move(libs));

  return true;
}

bool tvm_emulator_set_c7(void *tvm_emulator, const char *address, uint32_t unixtime, uint64_t balance, const char *rand_seed_hex, const char *config_boc) {
  auto emulator = static_cast<emulator::TvmEmulator *>(tvm_emulator);
  auto std_address = block::StdAddress::parse(td::Slice(address));
  if (std_address.is_error()) {
    LOG(ERROR) << "Can't parse address: " << std_address.move_as_error();
    return false;
  }
  
  auto config_params_decoded = td::base64_decode(td::Slice(config_boc));
  if (config_params_decoded.is_error()) {
    LOG(ERROR) << "Can't decode base64 config params boc: " << config_params_decoded.move_as_error();
    return false;
  }
  auto config_params_cell = vm::std_boc_deserialize(config_params_decoded.move_as_ok());
  if (config_params_cell.is_error()) {
    LOG(ERROR) << "Can't deserialize config params boc: " << config_params_cell.move_as_error();
    return false;
  }
  auto global_config = std::make_shared<block::Config>(config_params_cell.move_as_ok(), td::Bits256::zero(), block::Config::needWorkchainInfo | block::Config::needSpecialSmc);
  auto unpack_res = global_config->unpack();
  if (unpack_res.is_error()) {
    LOG(ERROR) << "Can't unpack config params";
    return false;
  }

  auto rand_seed_hex_slice = td::Slice(rand_seed_hex);
  if (rand_seed_hex_slice.size() != 64) {
    LOG(ERROR) << "Rand seed expected as 64 characters hex string";
    return false;
  }
  auto rand_seed_bytes = td::hex_decode(rand_seed_hex_slice);
  if (rand_seed_bytes.is_error()) {
    LOG(ERROR) << "Can't decode hex rand seed";
    return false;
  }
  td::BitArray<256> rand_seed;
  rand_seed.as_slice().copy_from(rand_seed_bytes.move_as_ok());

  emulator->set_c7(std_address.move_as_ok(), unixtime, balance, rand_seed, std::const_pointer_cast<const block::Config>(global_config));
  
  return true;
}

bool tvm_emulator_set_gas_limit(void *tvm_emulator, int64_t gas_limit) {
  auto emulator = static_cast<emulator::TvmEmulator *>(tvm_emulator);
  emulator->set_gas_limit(gas_limit);
  return true;
}

const char *tvm_emulator_run_get_method(void *tvm_emulator, int method_id, const char *stack_json_raw) {
  std::string stack_json_str(stack_json_raw);
  auto stack_json = td::json_decode(stack_json_str);
  if (stack_json.is_error()) {
    return error_response(PSTRING() << "Couldn't decode stack json: " << stack_json.move_as_error().to_string());
  }
  if (stack_json.ok_ref().type() != td::JsonValue::Type::Array) {
    return error_response(PSTRING() << "Stack of type array expected");
  }
  auto& stack_json_array = stack_json.ok_ref().get_array();
  std::vector<vm::StackEntry> stack_entries;
  for (auto& stack_entry_json : stack_json_array) {
    auto stack_entry = from_emulator_api(stack_entry_json);
    if (stack_entry.is_error()) {
      return error_response(PSTRING() << "Error parsing stack: " << stack_entry.move_as_error().to_string());
    }
    stack_entries.push_back(stack_entry.move_as_ok());
  }

  td::Ref<vm::Stack> stack(true, std::move(stack_entries));
  auto emulator = static_cast<emulator::TvmEmulator *>(tvm_emulator);
  auto result = emulator->run_get_method(method_id, stack);

  td::JsonBuilder jb;
  auto json_obj = jb.enter_object();
  json_obj("success", td::JsonTrue());
  json_obj("stack", StackJsonable(result.stack));
  json_obj("gas_used", std::to_string(result.gas_used));
  json_obj("vm_exit_code", result.code);
  json_obj("vm_log", result.vm_log);
  if (result.missing_library.is_null()) {
    json_obj("missing_library", td::JsonNull());
  } else {
    json_obj("missing_library", td::Bits256(result.missing_library).to_hex());
  }
  json_obj.leave();
  auto json_response = jb.string_builder().as_cslice().str();
  auto heap_response = new std::string(json_response);
  return heap_response->c_str();
}

void tvm_emulator_destroy(void *tvm_emulator) {
  delete static_cast<emulator::TvmEmulator *>(tvm_emulator);
}
