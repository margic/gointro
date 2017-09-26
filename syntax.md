Go takes an unusual approach to defining the visibility of an identifier, the ability for a client of a package to use the item named by the identifier. Unlike, for instance, private and public keywords, in Go the name itself carries the information: the case of the initial letter of the identifier determines the visibility. If the initial character is an upper case letter, the identifier is exported (public); otherwise it is not:

upper case initial letter: Name is visible to clients of package
otherwise: name (or _Name) is not visible to clients of package


Libraries use the error type to return a description of the error. Combined with the ability for functions to return multiple values, it's easy to return the computed result along with an error value, if any. For instance, the equivalent to C's getchar does not return an out-of-band value at EOF, nor does it throw an exception; it just returns an error value alongside the character, with a nil error value signifying success. Here is the signature of the ReadByte method of the buffered I/O package's bufio.Reader type:

func (b *Reader) ReadByte() (c byte, err error)
This is a clear and simple design, easily understood. Errors are just values and programs compute with them as they would compute with values of any other type.
