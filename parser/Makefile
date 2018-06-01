interp : logparser.hs
	ghc logparser.hs -main-is LogParser.main -o $@ 

clean :
	rm interp
	rm *.hi *.o logParser.hs

.PHONY: test clean