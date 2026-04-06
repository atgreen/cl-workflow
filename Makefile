cl-hackflow: src/*.lisp *.asd
	sbcl --eval "(asdf:make :cl-hackflow)" --quit

clean:
	rm -rf *~ cl-hackflow
