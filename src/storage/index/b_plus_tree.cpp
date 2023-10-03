#include <sstream>
#include <string>
#include <algorithm>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/* 
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  return root_page->root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;
  // Get root page id
  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();
  page_id_t page_id_to_fetch = header_page->root_page_id_;
  ctx.read_set_.emplace_back(std::move(header_guard));
  do {
    // If page_id_to_fetch is invalid, return false
    if (page_id_to_fetch == INVALID_PAGE_ID) {
      return false;
    }
    // Fetch page and Unlock old pages
    ReadPageGuard guard = bpm_->FetchPageRead(page_id_to_fetch);
    while (!ctx.read_set_.empty()) { ctx.read_set_.pop_front(); }
    // If page is a leaf page
    auto page = guard.As<BPlusTreePage>();
    if (page->IsLeafPage()) {
      auto leaf_page = guard.As<LeafPage>();
      bool flag = false;
      // FIXME: use binary search
      for (int i = 0; i < leaf_page->GetSize(); ++i) {
        if (comparator_(leaf_page->KeyAt(i), key) == 0) {
          result->push_back(leaf_page->ValueAt(i));
          flag = true;
        }
      }
      return flag; 
    }
    // Else if page is an internal page
    auto internal_page = guard.As<InternalPage>();
    // find the first key greater than *key*
    int index = 1;
    while (index < internal_page->GetSize() && 
          comparator_(internal_page->KeyAt(index), key) <= 0) { 
      ++index; 
    }
    page_id_to_fetch = internal_page->ValueAt(index - 1);
    // Add this page to read_set
    ctx.read_set_.emplace_back(std::move(guard));
  } while (true);
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;
  // Get root page id
  WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();

  // If tree is empty, create a new tree
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    auto root_guard = bpm_->NewPageGuarded(&header_page->root_page_id_);
    if (root_guard.GetData() == nullptr) {
      return false;
    }
    auto leaf_page = root_guard.AsMut<LeafPage>();
    leaf_page->Init(leaf_max_size_);
    leaf_page->InsertAt(key, value, 0);
    return true;
  }

  // If the tree is not empty, insert
  ctx.write_set_.emplace_back(std::move(header_guard));
  ctx.root_page_id_ = header_page->root_page_id_;
  return InsertRecursively(header_page->root_page_id_, ctx, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return 0; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

/*
 * This method is used for test only
 * Read data from file and insert/remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BatchOpsFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  char instruction;
  std::ifstream input(file_name);
  while (input) {
    input >> instruction >> key;
    RID rid(key);
    KeyType index_key;
    index_key.SetFromInteger(key);
    switch (instruction) {
      case 'i':
        Insert(index_key, rid, txn);
        break;
      case 'd':
        Remove(index_key, txn);
        break;
      default:
        break;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}



INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::UpperBound(const LeafPage* page, const KeyType& key) -> size_t {
  int index = 0;
  while (index < page->GetSize() && comparator_(page->KeyAt(index), key) <= 0) { ++index; }
  return index;
}


INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::UpperBound(const InternalPage* page, const KeyType& key) -> size_t {
  int index = 1;
  while (index < page->GetSize() && comparator_(page->KeyAt(index), key) <= 0) { ++index; }
  return index;
}


INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertRecursively(page_id_t page_id_to_fetch, Context &ctx, const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // fetch cur_page
  if (page_id_to_fetch == INVALID_PAGE_ID) {
    return false;
  }
  WritePageGuard cur_guard = bpm_->FetchPageWrite(page_id_to_fetch);
  auto cur_page = cur_guard.AsMut<BPlusTreePage>();
  // if header_page is not going to split, release locked pages before
  if (cur_page->GetSize() < cur_page->GetMaxSize() - 1) {
    while (!ctx.write_set_.empty()) { ctx.write_set_.pop_front(); }
  }

  // if cur_page is leaf_page, insert
  bool success = false;
  int index = -1;
  if (cur_page->IsLeafPage()) {
    auto leaf_page = cur_guard.AsMut<LeafPage>();
    index = UpperBound(leaf_page, key);
    success = leaf_page->InsertAt(key, value, index);
  // else if header_page is internal_page, walk through
  } else {
    auto internal_page = cur_guard.AsMut<InternalPage>();
    index = UpperBound(internal_page, key);
    ctx.write_set_.emplace_back(std::move(cur_guard));
    success = InsertRecursively(internal_page->ValueAt(index - 1), ctx, key, value);
    cur_guard = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
  }

  // if cur_page is not full, return
  if (cur_page->GetSize() < cur_page->GetMaxSize() || !success) {
    return success;
  }

  // else if cur_page is full: 1.split 2.update father
  // allocate a new page, for split
  page_id_t new_split_page_id = -1;
  auto new_split_page_guard = bpm_->NewPageGuarded(&new_split_page_id);
  BUSTUB_ASSERT(new_split_page_id != -1, "allocate error at split");
  // allocate a new page, for new root (if cur page is root page)
  page_id_t new_root_page_id = -1;
  BasicPageGuard new_root_guard;
  InternalPage* new_root_page = nullptr;
  if (ctx.IsRootPage(cur_guard.PageId())) {
    new_root_guard = bpm_->NewPageGuarded(&new_root_page_id);
    BUSTUB_ASSERT(new_root_page_id != -1, "allocate error at split");
    new_root_page = new_root_guard.AsMut<InternalPage>();
  }
  if (cur_page->IsLeafPage()) {
    // split
    auto cur_leaf_page = cur_guard.AsMut<LeafPage>();
    auto new_leaf_page = new_split_page_guard.AsMut<LeafPage>();
    int start = cur_leaf_page->GetSize() >> 1;
    int len = cur_leaf_page->GetSize() - start;
    cur_leaf_page->MoveTo(new_leaf_page, start, len, 0);
    // update father
    if (ctx.IsRootPage(cur_guard.PageId())) {
      auto& header_page_guard = ctx.write_set_.back();
      new_root_page->SetValueAt(cur_guard.PageId(), 0);
      success = new_root_page->InsertAt(new_leaf_page->KeyAt(0), new_split_page_id, 1);
      header_page_guard.AsMut<BPlusTreeHeaderPage>()->root_page_id_ = new_root_page_id;
    } else {
      auto& father_page_guard = ctx.write_set_.back();
      auto father_page = father_page_guard.AsMut<InternalPage>();
      int index = UpperBound(father_page, new_leaf_page->KeyAt(0));
      success = father_page->InsertAt(new_leaf_page->KeyAt(0), new_split_page_id, index);
    }
  } else {
    // split
    auto cur_internal_page = cur_guard.AsMut<InternalPage>();
    auto new_internal_page = new_split_page_guard.AsMut<InternalPage>();
    int start = cur_internal_page->GetSize() >> 1;
    int len = cur_internal_page->GetSize() - start;
    cur_internal_page->MoveTo(new_internal_page, start, len, 0);
    // update father
    if (ctx.IsRootPage(cur_guard.PageId())) {
      auto& header_page_guard = ctx.write_set_.back();
      new_root_page->SetValueAt(cur_guard.PageId(), 0);
      success = new_root_page->InsertAt(new_internal_page->KeyAt(0), new_split_page_id, 1);
      header_page_guard.AsMut<BPlusTreeHeaderPage>()->root_page_id_ = new_root_page_id;
    } else {
      auto& father_page_guard = ctx.write_set_.back();
      auto father_page = father_page_guard.AsMut<InternalPage>();
      int index = UpperBound(father_page, new_internal_page->KeyAt(0));
      success = father_page->InsertAt(new_internal_page->KeyAt(0), new_split_page_id, index);
    }
  }
  return success;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
