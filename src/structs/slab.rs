use anchor_lang::prelude::Pubkey;
use arrayref::{array_refs, mut_array_refs};
use bytemuck::{cast, cast_mut, cast_ref, cast_slice, cast_slice_mut, Pod, Zeroable};
use futures::join;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use solana_client::nonblocking::rpc_client::RpcClient;
use sqlx::types::Decimal;
use std::{
    convert::{identity, TryFrom},
    mem::{align_of, size_of},
    num::NonZeroU64, str::FromStr,
};

use crate::structs::openbook::token_factor;

use super::markets::MarketInfo;

pub type NodeHandle = u32;

#[derive(IntoPrimitive, TryFromPrimitive, Debug)]
#[repr(u32)]
enum NodeTag {
    Uninitialized = 0,
    InnerNode = 1,
    LeafNode = 2,
    FreeNode = 3,
    LastFreeNode = 4,
}

#[derive(Copy, Clone, IntoPrimitive, TryFromPrimitive, Debug)]
#[repr(u8)]
pub enum FeeTier {
    Base,
    _SRM2,
    _SRM3,
    _SRM4,
    _SRM5,
    _SRM6,
    _MSRM,
    Stable,
}

#[derive(Copy, Clone)]
#[repr(packed)]
#[allow(dead_code)]
struct InnerNode {
    tag: u32,
    prefix_len: u32,
    key: u128,
    children: [u32; 2],
    _padding: [u64; 5],
}
unsafe impl Zeroable for InnerNode {}
unsafe impl Pod for InnerNode {}

impl InnerNode {
    fn walk_down(&self, search_key: u128) -> (NodeHandle, bool) {
        let crit_bit_mask = (1u128 << 127) >> self.prefix_len;
        let crit_bit = (search_key & crit_bit_mask) != 0;
        (self.children[crit_bit as usize], crit_bit)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(packed)]
pub struct LeafNode {
    pub tag: u32,
    pub owner_slot: u8,
    pub fee_tier: u8,
    pub padding: [u8; 2],
    pub key: u128,
    pub owner: [u64; 4],
    pub quantity: u64,
    pub client_order_id: u64,
}
unsafe impl Zeroable for LeafNode {}
unsafe impl Pod for LeafNode {}

impl LeafNode {
    #[inline]
    pub fn new(
        owner_slot: u8,
        key: u128,
        owner: [u64; 4],
        quantity: u64,
        fee_tier: FeeTier,
        client_order_id: u64,
    ) -> Self {
        LeafNode {
            tag: NodeTag::LeafNode.into(),
            owner_slot,
            fee_tier: fee_tier.into(),
            padding: [0; 2],
            key,
            owner,
            quantity,
            client_order_id,
        }
    }

    #[inline]
    pub fn fee_tier(&self) -> FeeTier {
        FeeTier::try_from_primitive(self.fee_tier).unwrap()
    }

    #[inline]
    pub fn price(&self) -> NonZeroU64 {
        NonZeroU64::new((self.key >> 64) as u64).unwrap()
    }

    #[inline]
    pub fn order_id(&self) -> u128 {
        self.key
    }

    #[inline]
    pub fn quantity(&self) -> u64 {
        self.quantity
    }

    #[inline]
    pub fn set_quantity(&mut self, quantity: u64) {
        self.quantity = quantity;
    }

    #[inline]
    pub fn owner(&self) -> [u64; 4] {
        self.owner
    }

    #[inline]
    pub fn owner_slot(&self) -> u8 {
        self.owner_slot
    }

    #[inline]
    pub fn client_order_id(&self) -> u64 {
        self.client_order_id
    }
}

#[derive(Copy, Clone)]
#[repr(packed)]
#[allow(dead_code)]
struct FreeNode {
    tag: u32,
    next: u32,
    _padding: [u64; 8],
}
unsafe impl Zeroable for FreeNode {}
unsafe impl Pod for FreeNode {}

const fn _const_max(a: usize, b: usize) -> usize {
    let gt = (a > b) as usize;
    gt * a + (1 - gt) * b
}

const _INNER_NODE_SIZE: usize = size_of::<InnerNode>();
const _LEAF_NODE_SIZE: usize = size_of::<LeafNode>();
const _FREE_NODE_SIZE: usize = size_of::<FreeNode>();
const _NODE_SIZE: usize = 72;

const _INNER_NODE_ALIGN: usize = align_of::<InnerNode>();
const _LEAF_NODE_ALIGN: usize = align_of::<LeafNode>();
const _FREE_NODE_ALIGN: usize = align_of::<FreeNode>();
const _NODE_ALIGN: usize = 1;

#[derive(Copy, Clone, Debug)]
#[repr(packed)]
#[allow(dead_code)]
pub struct AnyNode {
    tag: u32,
    data: [u32; 17],
}
unsafe impl Zeroable for AnyNode {}
unsafe impl Pod for AnyNode {}

enum NodeRef<'a> {
    Inner(&'a InnerNode),
    Leaf(&'a LeafNode),
}

enum NodeRefMut<'a> {
    Inner(&'a mut InnerNode),
    Leaf(&'a mut LeafNode),
}

impl AnyNode {
    fn key(&self) -> Option<u128> {
        match self.case()? {
            NodeRef::Inner(inner) => Some(inner.key),
            NodeRef::Leaf(leaf) => Some(leaf.key),
        }
    }

    fn prefix_len(&self) -> u32 {
        match self.case().unwrap() {
            NodeRef::Inner(&InnerNode { prefix_len, .. }) => prefix_len,
            NodeRef::Leaf(_) => 128,
        }
    }

    fn children(&self) -> Option<[u32; 2]> {
        match self.case().unwrap() {
            NodeRef::Inner(&InnerNode { children, .. }) => Some(children),
            NodeRef::Leaf(_) => None,
        }
    }

    fn case(&self) -> Option<NodeRef> {
        match NodeTag::try_from(self.tag) {
            Ok(NodeTag::InnerNode) => Some(NodeRef::Inner(cast_ref(self))),
            Ok(NodeTag::LeafNode) => Some(NodeRef::Leaf(cast_ref(self))),
            _ => None,
        }
    }

    fn case_mut(&mut self) -> Option<NodeRefMut> {
        match NodeTag::try_from(self.tag) {
            Ok(NodeTag::InnerNode) => Some(NodeRefMut::Inner(cast_mut(self))),
            Ok(NodeTag::LeafNode) => Some(NodeRefMut::Leaf(cast_mut(self))),
            _ => None,
        }
    }

    #[inline]
    pub fn as_leaf(&self) -> Option<&LeafNode> {
        match self.case() {
            Some(NodeRef::Leaf(leaf_ref)) => Some(leaf_ref),
            _ => None,
        }
    }

    #[inline]
    pub fn as_leaf_mut(&mut self) -> Option<&mut LeafNode> {
        match self.case_mut() {
            Some(NodeRefMut::Leaf(leaf_ref)) => Some(leaf_ref),
            _ => None,
        }
    }
}

impl AsRef<AnyNode> for InnerNode {
    fn as_ref(&self) -> &AnyNode {
        cast_ref(self)
    }
}

impl AsRef<AnyNode> for LeafNode {
    #[inline]
    fn as_ref(&self) -> &AnyNode {
        cast_ref(self)
    }
}

#[derive(Copy, Clone, Debug)]
#[repr(packed)]
struct SlabHeader {
    _bump_index: u64,
    _free_list_len: u64,
    _free_list_head: u32,

    root_node: u32,
    leaf_count: u64,
}
unsafe impl Zeroable for SlabHeader {}
unsafe impl Pod for SlabHeader {}

const SLAB_HEADER_LEN: usize = size_of::<SlabHeader>();

#[cfg(debug_assertions)]
unsafe fn invariant(check: bool) {
    if check {
        unreachable!();
    }
}

#[cfg(not(debug_assertions))]
#[inline(always)]
unsafe fn invariant(check: bool) {
    if check {
        std::hint::unreachable_unchecked();
    }
}

/// Mainly copied from the original code, slightly modified to make working with it easier.
#[repr(transparent)]
pub struct Slab([u8]);

impl Slab {
    /// Creates a slab that holds and references the bytes
    ///
    /// ```compile_fail
    /// let slab = {
    ///     let mut bytes = [10; 100];
    ///     serum_dex::critbit::Slab::new(&mut bytes)
    /// };
    /// ```
    #[inline]
    pub fn new(raw_bytes: &mut [u8]) -> &mut Self {
        let data_end = raw_bytes.len() - 7;
        let bytes = &mut raw_bytes[13..data_end];
        let len_without_header = bytes.len().checked_sub(SLAB_HEADER_LEN).unwrap();
        let slop = len_without_header % size_of::<AnyNode>();
        let truncated_len = bytes.len() - slop;
        let bytes = &mut bytes[..truncated_len];
        let slab: &mut Self = unsafe { &mut *(bytes as *mut [u8] as *mut Slab) };
        slab.check_size_align(); // check alignment
        slab
    }

    pub fn get(&self, key: u32) -> Option<&AnyNode> {
        let node = self.nodes().get(key as usize)?;
        let tag = NodeTag::try_from(node.tag);
        match tag {
            Ok(NodeTag::InnerNode) | Ok(NodeTag::LeafNode) => Some(node),
            _ => None,
        }
    }

    fn check_size_align(&self) {
        let (header_bytes, nodes_bytes) = array_refs![&self.0, SLAB_HEADER_LEN; .. ;];
        let _header: &SlabHeader = cast_ref(header_bytes);
        let _nodes: &[AnyNode] = cast_slice(nodes_bytes);
    }

    fn parts(&self) -> (&SlabHeader, &[AnyNode]) {
        unsafe {
            invariant(self.0.len() < size_of::<SlabHeader>());
            invariant((self.0.as_ptr() as usize) % align_of::<SlabHeader>() != 0);
            invariant(
                ((self.0.as_ptr() as usize) + size_of::<SlabHeader>()) % align_of::<AnyNode>() != 0,
            );
        }

        let (header_bytes, nodes_bytes) = array_refs![&self.0, SLAB_HEADER_LEN; .. ;];
        let header = cast_ref(header_bytes);
        let nodes = cast_slice(nodes_bytes);
        (header, nodes)
    }

    fn header(&self) -> &SlabHeader {
        self.parts().0
    }

    fn nodes(&self) -> &[AnyNode] {
        self.parts().1
    }

    fn root(&self) -> Option<NodeHandle> {
        if self.header().leaf_count == 0 {
            return None;
        }

        Some(self.header().root_node)
    }

    fn find_min_max(&self, find_max: bool) -> Option<NodeHandle> {
        let mut root: NodeHandle = self.root()?;
        loop {
            let root_contents = self.get(root).unwrap();
            match root_contents.case().unwrap() {
                NodeRef::Inner(&InnerNode { children, .. }) => {
                    root = children[if find_max { 1 } else { 0 }];
                    continue;
                }
                _ => return Some(root),
            }
        }
    }

    #[inline]
    pub fn find_min(&self) -> Option<&LeafNode> {
        let handle = self.find_min_max(false).unwrap();
        match self.get(handle) {
            Some(node) => Some(node.as_leaf().unwrap()),
            None => None,
        }
    }

    #[inline]
    pub fn find_max(&self) -> Option<&LeafNode> {
        let handle = self.find_min_max(true).unwrap();
        match self.get(handle) {
            Some(node) => Some(node.as_leaf().unwrap()),
            None => None,
        }
    }

    pub fn get_best(&self, market: &MarketInfo, bid: bool) -> Decimal {
        let min = if bid {
            self.find_max()
        } else {
            self.find_min()
        };

        let price_lots = Decimal::from(min.unwrap().key >> 64);
        let base_multiplier = token_factor(market.base_decimals);
        let quote_multiplier = token_factor(market.quote_decimals);
        let base_lot_size = Decimal::from(market.base_lot_size);
        let quote_lot_size = Decimal::from(market.quote_lot_size);
        (price_lots * quote_lot_size * base_multiplier) / (base_lot_size * quote_multiplier)
    }
}

pub async fn get_best_bids_and_asks(
    client: RpcClient,
    markets: &Vec<MarketInfo>,
) -> (Vec<Decimal>, Vec<Decimal>) {
    let bid_keys = markets
        .iter()
        .map(|m| Pubkey::from_str(&m.bids_key).unwrap())
        .collect::<Vec<Pubkey>>();
    let ask_keys = markets
        .iter()
        .map(|m| Pubkey::from_str(&m.asks_key).unwrap())
        .collect::<Vec<Pubkey>>();

    // will error if more than 100 markets are used (not a good idea in general)
    let (bid_results, ask_results) = join!(
        client.get_multiple_accounts(&bid_keys),
        client.get_multiple_accounts(&ask_keys)
    );

    let bids = bid_results.unwrap();
    let asks = ask_results.unwrap();

    let best_bids = bids
        .into_iter()
        .enumerate()
        .filter_map(|(index, x)| {
            if let Some(mut account) = x {
                let slab = Slab::new(&mut account.data);
                Some(slab.get_best(&markets[index], true))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    let best_asks = asks
        .into_iter()
        .enumerate()
        .filter_map(|(index, x)| {
            if let Some(mut account) = x {
                let slab = Slab::new(&mut account.data);
                Some(slab.get_best(&markets[index], false))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    (best_bids, best_asks)
}
