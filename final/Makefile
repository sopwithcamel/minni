# Setup
TEX=pdflatex
BIB=bibtex
NAME=final

all: pdf

pdf: $(NAME).tex references.bib
	@$(TEX) $(NAME).tex
	@$(BIB) $(NAME)
	@$(TEX) $(NAME).tex
	@$(TEX) $(NAME).tex	

clean:
	@rm $(NAME).aux $(NAME).log $(NAME).bbl $(NAME).blg $(NAME).pdf
	
