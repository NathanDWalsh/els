# About #

This is the Informatics Project Proposal assignment document.

The skeleton contents were written by J Douglas Armstrong.
The LaTeX content was written by Brian Mitchell.

The document is configurable for accessibility. It produces a PDF that has accessibility features, though it is the user's (that is, the document author's) responsibility to write and include alternative text descriptions.

This LaTeX document will only compile with a version of lualatex dating from 2021. You have no hope of compiling it with pdftex.

You are advised to compile the document with latexmk (which requires Perl). It is far easier to compile this document on Overleaf than locally.

# Editing the coursework document #

All these instructions apply to [`IPPreport.tex`](IPPreport.tex) unless otherwise stated.

# Meta data #

## Document title ##

For the title use Title Case Where Most Words Are Initial-Caps.

Long titles need splitting across 2 or 3 lines using "\\ " (with the space)
Try not to split a single noun phrase or verb phrase across lines.

## Supervisor and IPP tutor ##

Titles in English are used with the family name not the given ("first") name. Titles in British English do not have a fullstop ("period") after them. In LaTeX, the title is joined to the name with a ~ (tilde) character. This is LaTeX's way of specifying a non-breaking space.

Ensure you give staff their appropriate title: don't "downgrade" them accidentally.

## Keywords ##
If you want keywords, then add them comma-separated:
\newcommand*{\KEYWORDS}{Human-Computer Interaction, HCI, Graphical User Interface, GUI, User Experience, UX}

# Abstract #

The abstract is in [`abstract.tex`](abstract.tex).

Try to keep your abstract to about 100 words, though you might be able to make it shorter than that if your writing style is efficient.

# Referencing #

The defult referencing style is Harvard (the source says `bath` because the LaTeX package for Harvard referencing is written by the library of the University of Bath).

If your supervisor wants numeric style, then comment out the Harvard line and uncomment the IEEE line.

# Accessibility #

## Colours ##

You may configure the PDF's colour scheme in colours.tex. You may reconfigure the document if you have dyslexia, low-vision, or photo-sensitivty. Check with your supervisor and IPP tutor to see what colour scheme they prefer (if any). You may use a different colour theme to suit yourself, for example a dark theme, while you are wriitng the document.

## Font ##

The default font is designed for screen-reading and it also decent when printed. If you need to change the font, either for yourself or supervisor, then you can uncomment the example and change the font name. There is [a list of fonts supported by Overleaf](https://www.overleaf.com/learn/latex/Questions/Which_OTF_or_TTF_fonts_are_supported_via_fontspec%3F).

## Line spacing ##

You may change the line spacing for readability. Check with your supervisor and IPP tutor to see what they prefer. Use may use a different spacing while you are writing the document.

LaTeX has separate commands for the line spacing of the main document, lines in tables, and lines in captions (for tables, figures, and possibly other items).

## Line justification ##

By default paragraphs are fully justified (margin to margin). If you prefer, you may configure the document to be left-justified (known as "ragged right" in LaTeX), and you have two choices here: with a small amount of hyphenation or no hyphenation.

## Hyphenation of words ##

The document contains an example of overriding how words can be hyphenated using the `\pghyphenation` command.

# Packages #

Try really hard not to add any LaTeX packages: the common useful ones are already loaded.

If you must add a pacakge, then put it in the existing file [`_extraPackages.tex`](_extraPackages.tex) and be sure to load the package in the appropriate place. You MUST read the package's manual to see whether it must be loaded before biblatex, before hyperref, or after hyperref. Loading packages in the wrong order can cause a world of pain.

# Footnotes #

No, you are not seeing things. This document changes footnotes into side notes (appearing in the margin) because that layout is much better for readability and accessibility.

There is a line in the document that lets you see the warning messages about side notes being moved when a document is recompiled. You can almost always safely ignore these "warnings" which is why they are suppressed by default.

# The main document #

Start editing the document from `\section{Introduction}`.

Whenever you add a section or subsection (try really hard not to use subsubsections) always add a label: for example
\section{Introduction}%
\label{sec:introduction}

(For convenience, always use `sec:` as the prefix no matter whether it is a chapter, section, or subsection). Now you can reference (sub)sections with `\cref{sec:introductin}` (for the section number or `\nameref{sec:introduction}` for the actual text of the section's title. This will automatically be hyperlinked. Magic!

You are advised to use camelCase for labels rather than snake_case because the underscore character has special meaning in LaTeX and the Overleaf editor is easily confused and upset.

# Automatically numbered description lists #

If you want a description list that is numbered automatically, then use

```latex
\begin{enumdescript}[nosep]
\item[description 1] description 1 text
\item[description 2] description 2 text
\end{enumdescript}
```

See [the `enumitem` manual](https://ctan.org/pkg/enumitem) for more options with `itemize`, `enumerate`, and `description` lists.

# Tables #

The default table template in Overleaf is bad practice. Tables should NOT have vertical lines, nor should use the `\hline`. The tables in the skeleton content show the professional and technically correct way to do tables in LaTeX.
