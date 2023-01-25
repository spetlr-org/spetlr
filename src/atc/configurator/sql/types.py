from more_itertools import peekable

# This type should express that it is a peekable iterator
# that returns TokenLists. It seems that this is not easily
# expressed. Here is what I had tried:
# peekable[TokenList]
_PeekableTokenList = peekable
