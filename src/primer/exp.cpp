#include <iostream>
#include <string_view>

#include "../include/primer/trie.h"

int main()
{
  bustub::Trie t;
  t.Put("hello", 5);
  t.Put("world", 10);
  t.Put("hell", 15);
  t.Put("hell", 20);
  t.Put("hell", 25);
  std::cout << *t.Get<int>("hello") << std::endl;
  std::cout << *t.Get<int>("world") << std::endl;
  std::cout << *t.Get<int>("hell") << std::endl;
  return 0;
}