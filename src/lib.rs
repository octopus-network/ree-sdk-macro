use proc_macro::TokenStream;
use quote::{format_ident, quote};
use std::collections::BTreeMap;
use syn::{
    parse_macro_input, parse_quote, visit_mut::VisitMut, Attribute, Ident, ItemFn, ItemMod,
    ItemStruct, Meta,
};

#[derive(Clone)]
struct CanisterVisitor {
    commits: BTreeMap<String, String>,
    pools: Option<Ident>,
}

impl CanisterVisitor {
    fn new() -> Self {
        CanisterVisitor {
            commits: BTreeMap::new(),
            pools: None,
        }
    }

    fn resolve_pools(&mut self, ty: &ItemStruct) {
        let mark_pools = ty.attrs.iter().find(|a| a.path().is_ident("pools"));
        if mark_pools.is_none() {
            return;
        }
        if self.pools.is_some() {
            panic!("Only one struct can have the #[pools] attribute");
        }
        self.pools = Some(ty.ident.clone());
    }

    fn resolve_action(&mut self, attr: &Attribute, func: &ItemFn) {
        let is_commit = attr.path().is_ident("commit");
        if !is_commit {
            return;
        }
        if let Meta::Path(_) = &attr.meta {
            self.commits
                .insert(func.sig.ident.to_string(), func.sig.ident.to_string());
        } else if let Meta::List(meta_list) = &attr.meta {
            let exp = meta_list.tokens.clone().into_iter().collect::<Vec<_>>();
            if exp.is_empty() {
                self.commits
                    .insert(func.sig.ident.to_string(), func.sig.ident.to_string());
            } else if exp.len() == 1 {
                if let proc_macro2::TokenTree::Literal(lit) = &exp[0] {
                    let action = lit.to_string().replace('"', "");
                    self.commits.insert(action, func.sig.ident.to_string());
                } else {
                    panic!("Expected #[commit(\"...\")] attribute");
                }
            } else if exp.len() == 3 {
                let b0 = matches!(exp[0].clone(), proc_macro2::TokenTree::Ident(ident) if ident.to_string() == "action");
                let b1 = matches!(exp[1].clone(), proc_macro2::TokenTree::Punct(punct) if punct.as_char() == '=');
                if !(b0 && b1) {
                    panic!("Expected #[commit(action = \"...\")] attribute");
                }
                if let proc_macro2::TokenTree::Literal(lit) = &exp[2] {
                    let action = lit.to_string().replace('"', "");
                    self.commits.insert(action, func.sig.ident.to_string());
                } else {
                    panic!("Expected #[commit(action = \"...\")] attribute");
                }
            } else {
                panic!("Unexpected tokens in #[commit] macro");
            }
        } else {
            panic!("Expected `#[commit(\"..\")]` or `#[commit(action = \"..\")]` or `#[commit]` attribute");
        }
    }
}

impl VisitMut for CanisterVisitor {
    fn visit_item_fn_mut(&mut self, item: &mut syn::ItemFn) {
        for attr in item.attrs.iter() {
            self.resolve_action(&attr, item);
        }
        syn::visit_mut::visit_item_fn_mut(self, item);
    }

    fn visit_item_struct_mut(&mut self, item: &mut ItemStruct) {
        self.resolve_pools(item);
        syn::visit_mut::visit_item_struct_mut(self, item);
    }
}

#[proc_macro_attribute]
pub fn exchange(attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut input_mod = parse_macro_input!(item as ItemMod);
    let mut visitor = CanisterVisitor::new();
    visitor.visit_item_mod_mut(&mut input_mod);
    if visitor.pools.is_none() {
        panic!("#[pools] not found within the exchange mod");
    }
    let testnet = attr
        .into_iter()
        .filter_map(|a| match a {
            proc_macro::TokenTree::Ident(ident) if ident.to_string() == "testnet" => {
                Some(ident.clone())
            }
            _ => None,
        })
        .next()
        .is_some();
    let authentication = if testnet {
        quote! {  <::candid::Principal as std::str::FromStr>::from_str("hvyp5-5yaaa-aaaao-qjxha-cai").unwrap() }
    } else {
        quote! {  <::candid::Principal as std::str::FromStr>::from_str("kqs64-paaaa-aaaar-qamza-cai").unwrap() }
    };
    let pools = visitor.pools.clone().unwrap();
    if let Some((_, ref mut items)) = input_mod.content {
        let branch = visitor
            .commits
            .iter()
            .map(|(k, v)| {
                let call = format_ident!("{}", v);
                quote! { #k => #call(args), }
            })
            .collect::<Vec<_>>();

        items.push(parse_quote! {
            impl #pools {
                pub fn orchestrator() -> ::candid::Principal {
                    #authentication
                }
            }
        });

        items.push(parse_quote! {
            impl ::ree_types::exchange_interfaces::PoolStorage<#pools> for #pools {
                fn pool(address: &String) -> Option<<#pools as ::ree_types::exchange_interfaces::Pools>::Pool> {
                    self::CURRENT_POOLS.with_borrow(|p| p.get(address))
                }

                fn put(pool: <#pools as ::ree_types::exchange_interfaces::Pools>::Pool) {
                    self::CURRENT_POOLS.with_borrow_mut(|p| {
                        p.insert(pool.address.clone(), pool);
                    });
                }

                fn remove(address: &String) {
                    self::CURRENT_POOLS.with_borrow_mut(|p| {
                        p.remove(address);
                    });
                }
            }
        });

        items.push(parse_quote! {
            #[::ic_cdk::update]
            pub async fn execute_tx(args: ::ree_types::exchange_interfaces::ExecuteTxArgs) -> ::core::result::Result<String, String> {
                let orchestrator = #pools::orchestrator();
                (::ic_cdk::caller() == orchestrator)
                    .then(|| ())
                    .ok_or("Only Orchestrator can call this function".to_string())?;

                let ::ree_types::exchange_interfaces::ExecuteTxArgs {
                    psbt_hex,
                    txid,
                    intention_set,
                    intention_index,
                    zero_confirmed_tx_queue_length,
                } = args;
                let ::ree_types::Intention {
                    exchange_id,
                    action,
                    action_params,
                    pool_address,
                    nonce,
                    pool_utxo_spent,
                    pool_utxo_received,
                    input_coins,
                    output_coins,
                } = &intention_set.intentions[intention_index as usize];
                let __pool_address = pool_address.clone();
                let __txid = txid.clone();
                let __action = action.clone();
                let args = ::ree_types::exchange_interfaces::ExecuteTxArgs {
                    psbt_hex,
                    txid,
                    intention_set,
                    intention_index,
                    zero_confirmed_tx_queue_length,
                };
                let __result = match __action.as_str() {
                    #(#branch)*
                    _ => ::core::result::Result::<String, String>::Err(format!("Unknown action: {}", __action)),
                };
                match __result {
                    ::core::result::Result::Ok(r) => {
                        self::TX_RECORDS.with_borrow_mut(|m| {
                            let mut __record = m.get(&(__txid.clone(), false)).unwrap_or_default();
                            if !__record.pools.contains(&__pool_address) {
                                __record.pools.push(__pool_address.clone());
                            }
                            m.insert((__txid.clone(), false), __record);
                        });
                        ::core::result::Result::<String, String>::Ok(r)
                    }
                    ::core::result::Result::Err(e) => ::core::result::Result::<String, String>::Err(e),
                }
            }
        });

        items.push(parse_quote! {
            #[::ic_cdk::query]
            pub fn get_pool_list() -> ::ree_types::exchange_interfaces::GetPoolListResponse {
                self::CURRENT_POOLS.with_borrow(|p| {
                    p.iter()
                        .map(|(_id, p)| {
                            <<#pools as ::ree_types::exchange_interfaces::Pools>::Pool as ::ree_types::exchange_interfaces::ReePool>::get_basic_info(&p)
                        })
                        .collect::<Vec<_>>()
                })
            }
        });

        items.push(parse_quote! {
            #[::ic_cdk::query]
            pub fn get_pool_info(
                args: ::ree_types::exchange_interfaces::GetPoolInfoArgs,
            ) -> ::ree_types::exchange_interfaces::GetPoolInfoResponse {
                self::CURRENT_POOLS.with_borrow(|p| {
                    p.get(&args.pool_address).map(|pool| {
                        <<#pools as ::ree_types::exchange_interfaces::Pools>::Pool as ::ree_types::exchange_interfaces::ReePool>::get_pool_info(&pool)
                    })
                })
            }
        });

        items.push(parse_quote! {
            #[::ic_cdk::update]
            pub fn rollback_tx(
                args: ::ree_types::exchange_interfaces::RollbackTxArgs,
            ) -> ::ree_types::exchange_interfaces::RollbackTxResponse {
                let orchestrator = #pools::orchestrator();
                (::ic_cdk::caller() == orchestrator)
                    .then(|| ())
                    .ok_or("Only Orchestrator can call this function".to_string())?;
                self::TX_RECORDS.with_borrow_mut(|transactions| {
                    self::CURRENT_POOLS.with_borrow_mut(|pools| {
                        ::ree_types::reorg::rollback_tx(transactions, pools, args)
                    })
                })
            }
        });

        items.push(parse_quote! {
            #[::ic_cdk::update]
            pub fn new_block(
                args: ::ree_types::exchange_interfaces::NewBlockArgs,
            ) -> ::ree_types::exchange_interfaces::NewBlockResponse {
                let orchestrator = #pools::orchestrator();
                (::ic_cdk::caller() == orchestrator)
                    .then(|| ())
                    .ok_or("Only Orchestrator can call this function".to_string())?;
                self::TX_RECORDS.with_borrow_mut(|transactions| {
                    self::CURRENT_POOLS.with_borrow_mut(|pools| {
                        self::BLOCKS.with_borrow_mut(|blocks| {
                            ::ree_types::reorg::new_block(
                                blocks,
                                transactions,
                                pools,
                                <#pools as ::ree_types::exchange_interfaces::Pools>::finalize_threshold(),
                                args
                            )
                        })
                    })
                })
            }
        });

        items.push(parse_quote! {
            thread_local! {
                static MEMORY_MANAGER: ::core::cell::RefCell<
                    ::ic_stable_structures::memory_manager::MemoryManager<
                        ::ic_stable_structures::DefaultMemoryImpl
                    >
                > = ::core::cell::RefCell::new(
                    ::ic_stable_structures::memory_manager::MemoryManager::init(
                        <::ic_stable_structures::DefaultMemoryImpl as core::default::Default>::default()
                    )
                );
                static BLOCKS: ::core::cell::RefCell<
                    ::ic_stable_structures::StableBTreeMap<
                        u32,
                        ::ree_types::exchange_interfaces::NewBlockInfo,
                        ::ic_stable_structures::memory_manager::VirtualMemory<::ic_stable_structures::DefaultMemoryImpl>
                    >
                > = ::core::cell::RefCell::new(
                    ::ic_stable_structures::StableBTreeMap::init(
                        MEMORY_MANAGER.with(|m| m.borrow().get(::ic_stable_structures::memory_manager::MemoryId::new(100))),
                    )
                );
                static TX_RECORDS: ::core::cell::RefCell<
                    ::ic_stable_structures::StableBTreeMap<
                        (::ree_types::Txid, bool),
                        ::ree_types::TxRecord,
                        ::ic_stable_structures::memory_manager::VirtualMemory<::ic_stable_structures::DefaultMemoryImpl>
                    >
                > = ::core::cell::RefCell::new(
                    ::ic_stable_structures::StableBTreeMap::init(
                        MEMORY_MANAGER.with(|m| m.borrow().get(::ic_stable_structures::memory_manager::MemoryId::new(101))),
                    )
                );
                static CURRENT_POOLS: ::core::cell::RefCell<
                    ::ic_stable_structures::StableBTreeMap<
                        ::std::string::String,
                        <#pools as ::ree_types::exchange_interfaces::Pools>::Pool,
                        ::ic_stable_structures::memory_manager::VirtualMemory<::ic_stable_structures::DefaultMemoryImpl>
                    >
                > = ::core::cell::RefCell::new(
                    ::ic_stable_structures::StableBTreeMap::init(
                        MEMORY_MANAGER.with(|m| m.borrow().get(::ic_stable_structures::memory_manager::MemoryId::new(102))),
                    )
                );
            }
        });
    }
    quote! {
        #input_mod
    }
    .into()
}

#[proc_macro_attribute]
pub fn commit(_attr: TokenStream, item: TokenStream) -> TokenStream {
    // TODO check func sig
    item
}

#[proc_macro_attribute]
pub fn pools(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}
