# Setup
TEX=pdflatex
BIB=bibtex

all: pdf

pdf: proposal.tex references.bib
	@$(TEX) proposal.tex
	@$(BIB) proposal
	@$(TEX) proposal.tex
	@$(TEX) proposal.tex	

clean:
	@rm proposal.aux proposal.log proposal.bbl proposal.blg proposal.pdf
	
