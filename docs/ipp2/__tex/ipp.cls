%! class = ipp
%! author = Brian Mitchell

% We want to be modern and use LaTeX3 so naturally we start with, er...
\NeedsTeXFormat{LaTeX2e}
\ProvidesExplClass{ipp}
{024/03/06} {1.0} {Class for IPP coursework (BM)}

\str_const:Nn \c__ipp_base_class_str     { article }
\str_const:Nn \c__ipp_base_setup_file    { _extraSetup.tex }
\str_const:Nn \c__ipp_base_packages_file { _extraPackages.tex }

\keys_define:nn { ipp }
  {
  , documentclass .str_set:N            = \l__ipp_class_str
  , documentclass .initial:V            = \c__ipp_base_class_str
  , documentclass .value_required:n     = true

  , unknown .code:n =
    {
    \iow_term:x
      {
      Passing~option~\CurrentOption \c_space_tl to~
      \str_use:N { \l__ipp_class_str } }
     }
    \PassOptionsToClass { \CurrentOption } { \l__ipp_class_str }
  }
\ProcessKeyOptions

\cs_new_protected:Npn \l__ipp_load_file:n #1
  {\iow_term:x {|Loading~#1}
    \file_if_exist:nTF { #1 }
      {
        \ExplSyntaxOff%
        \input{#1}%
        \ExplSyntaxOn
      }
      {
        \iow_term:x { File~#1~not~found }
      }
  }
\cs_generate_variant:Nn \l__ipp_load_file:n {v}

\l__ipp_load_file:n { \c__ipp_base_setup_file }
% \input{__tex/_extraSetup}
\LoadClass { \str_use:N \l__ipp_class_str }
\l__ipp_load_file:n { \c__ipp_base_packages_file }
% \input{__tex/_extraPackages}