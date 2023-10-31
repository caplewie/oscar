# oscar
Oscar is a Python library to for hashing files while allowing for interruptions. The primary use of this is in AWS Lambda functions where the functino may terminate before the file is fully hashed.
Resumption of the hash is provided for by the "rehash" library.

An example Lambda fucntion is provided.
