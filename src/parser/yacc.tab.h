/* A Bison parser, made by GNU Bison 3.8.2.  */

/* Bison interface for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015, 2018-2021 Free Software Foundation,
   Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <https://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* DO NOT RELY ON FEATURES THAT ARE NOT DOCUMENTED in the manual,
   especially those whose name start with YY_ or yy_.  They are
   private implementation details that can be changed or removed.  */

#ifndef YY_YY_MNT_D_RMDB_SRC_PARSER_YACC_TAB_H_INCLUDED
# define YY_YY_MNT_D_RMDB_SRC_PARSER_YACC_TAB_H_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int yydebug;
#endif

/* Token kinds.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    YYEMPTY = -2,
    YYEOF = 0,                     /* "end of file"  */
    YYerror = 256,                 /* error  */
    YYUNDEF = 257,                 /* "invalid token"  */
    SHOW = 258,                    /* SHOW  */
    TABLES = 259,                  /* TABLES  */
    CREATE = 260,                  /* CREATE  */
    TABLE = 261,                   /* TABLE  */
    DROP = 262,                    /* DROP  */
    DESC = 263,                    /* DESC  */
    INSERT = 264,                  /* INSERT  */
    INTO = 265,                    /* INTO  */
    VALUES = 266,                  /* VALUES  */
    DELETE = 267,                  /* DELETE  */
    FROM = 268,                    /* FROM  */
    ASC = 269,                     /* ASC  */
    ORDER = 270,                   /* ORDER  */
    BY = 271,                      /* BY  */
    UNION = 272,                   /* UNION  */
    WHERE = 273,                   /* WHERE  */
    GROUP = 274,                   /* GROUP  */
    HAVING = 275,                  /* HAVING  */
    LIMIT = 276,                   /* LIMIT  */
    UPDATE = 277,                  /* UPDATE  */
    SET = 278,                     /* SET  */
    SELECT = 279,                  /* SELECT  */
    COUNT = 280,                   /* COUNT  */
    MAX = 281,                     /* MAX  */
    MIN = 282,                     /* MIN  */
    SUM = 283,                     /* SUM  */
    AVG = 284,                     /* AVG  */
    INT = 285,                     /* INT  */
    CHAR = 286,                    /* CHAR  */
    FLOAT = 287,                   /* FLOAT  */
    INDEX = 288,                   /* INDEX  */
    AND = 289,                     /* AND  */
    JOIN = 290,                    /* JOIN  */
    ON = 291,                      /* ON  */
    EXIT = 292,                    /* EXIT  */
    HELP = 293,                    /* HELP  */
    TXN_BEGIN = 294,               /* TXN_BEGIN  */
    TXN_COMMIT = 295,              /* TXN_COMMIT  */
    TXN_ABORT = 296,               /* TXN_ABORT  */
    TXN_ROLLBACK = 297,            /* TXN_ROLLBACK  */
    ORDER_BY = 298,                /* ORDER_BY  */
    ENABLE_NESTLOOP = 299,         /* ENABLE_NESTLOOP  */
    ENABLE_SORTMERGE = 300,        /* ENABLE_SORTMERGE  */
    EXPLAIN = 301,                 /* EXPLAIN  */
    ANALYZE = 302,                 /* ANALYZE  */
    AS = 303,                      /* AS  */
    TRANSACTION = 304,             /* TRANSACTION  */
    ISOLATION = 305,               /* ISOLATION  */
    LEVEL = 306,                   /* LEVEL  */
    SNAPSHOT = 307,                /* SNAPSHOT  */
    SERIALIZABLE = 308,            /* SERIALIZABLE  */
    LEQ = 309,                     /* LEQ  */
    NEQ = 310,                     /* NEQ  */
    GEQ = 311,                     /* GEQ  */
    T_EOF = 312,                   /* T_EOF  */
    IDENTIFIER = 313,              /* IDENTIFIER  */
    VALUE_STRING = 314,            /* VALUE_STRING  */
    VALUE_INT = 315,               /* VALUE_INT  */
    VALUE_FLOAT = 316,             /* VALUE_FLOAT  */
    VALUE_BOOL = 317               /* VALUE_BOOL  */
  };
  typedef enum yytokentype yytoken_kind_t;
#endif

/* Value type.  */

/* Location type.  */
#if ! defined YYLTYPE && ! defined YYLTYPE_IS_DECLARED
typedef struct YYLTYPE YYLTYPE;
struct YYLTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
};
# define YYLTYPE_IS_DECLARED 1
# define YYLTYPE_IS_TRIVIAL 1
#endif




int yyparse (void *yyscanner);


#endif /* !YY_YY_MNT_D_RMDB_SRC_PARSER_YACC_TAB_H_INCLUDED  */
