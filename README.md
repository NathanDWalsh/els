# eel: easy extract load

First target use case is to create a decalrative language (eel.yaml) to quickly spin up a data project from multiple sources.

## easy

Ease of use and forgivable/optional syntax are a design priority. All required is a valid (file,folder,url) to get started. With only one data source/store declared, the contents of the file will be output to the screen.

## extract

By default, data defined will be the source.

## load

Load should be defined in an eel.yaml file. The location of this file will be assumed to be in the same folder as the file which defined the source.

## small-t transform

Some transformations will be supported, as the product matures more should be added. Some examples of mvp1 transforms:

* add index / unique id
* unpivot / melt data
* include file details
* include custom columns defined in eel.yaml

## eel.yaml

Optional, if not included then file content output to console. Otherwise:

* Defines source
* Explicit 
* Implicit same as yaml file name and in same directory
* Can be:
* Single file (csv, )
* Multiple files
* Multiple sheets from a single file
* read_sql
* read_csv
* read_parquet
* Define destination

Data store types for mvp1: (sql/csv/excel/parquet)

```dot
digraph {
a -> b
}
```

```plantuml
 @startuml thename

rectangle "Store Container: has other Containers or Objects" as sc {

    note as n1
        **examples**
        *database
        *schema
        *folder
        *workbook
        *url
        *api
        *service
    end note

    rectangle "Store Object" as so {



        rectangle "Store Attribute" as sa {
           note as n3
                **examples**
                *field
                *column
                *index
                *key
            end note
        }

        rectangle "Store Instance" as si {
           note as n4
                **examples**
                *row
                *scalar
                *value
            end note
        }

        

        note as n2
            **examples**
            *table
            *document
            *json
            *xml
            *file
            *csv
            *tsv
            *worksheet
        end note
    
    }


}



@enduml
```

```mermaid
C4Context
      title System Context diagram for Internet Banking System
      Enterprise_Boundary(b0, "BankBoundary0") {
        Person(customerA, "Banking Customer A", "A customer of the bank, with personal bank accounts.")
        Person(customerB, "Banking Customer B")
        Person_Ext(customerC, "Banking Customer C", "desc")

        Person(customerD, "Banking Customer D", "A customer of the bank, <br/> with personal bank accounts.")

        System(SystemAA, "Internet Banking System", "Allows customers to view information about their bank accounts, and make payments.")

        Enterprise_Boundary(b1, "BankBoundary") {

          SystemDb_Ext(SystemE, "Mainframe Banking System", "Stores all of the core banking information about customers, accounts, transactions, etc.")

          System_Boundary(b2, "BankBoundary2") {
            System(SystemA, "Banking System A")
            System(SystemB, "Banking System B", "A system of the bank, with personal bank accounts. next line.")
          }

          System_Ext(SystemC, "E-mail system", "The internal Microsoft Exchange e-mail system.")
          SystemDb(SystemD, "Banking System D Database", "A system of the bank, with personal bank accounts.")

          Boundary(b3, "BankBoundary3", "boundary") {
            SystemQueue(SystemF, "Banking System F Queue", "A system of the bank.")
            SystemQueue_Ext(SystemG, "Banking System G Queue", "A system of the bank, with personal bank accounts.")
          }
        }
      }

      BiRel(customerA, SystemAA, "Uses")
      BiRel(SystemAA, SystemE, "Uses")
      Rel(SystemAA, SystemC, "Sends e-mails", "SMTP")
      Rel(SystemC, customerA, "Sends e-mails to")

      UpdateElementStyle(customerA, $fontColor="red", $bgColor="grey", $borderColor="red")
      UpdateRelStyle(customerA, SystemAA, $textColor="blue", $lineColor="blue", $offsetX="5")
      UpdateRelStyle(SystemAA, SystemE, $textColor="blue", $lineColor="blue", $offsetY="-10")
      UpdateRelStyle(SystemAA, SystemC, $textColor="blue", $lineColor="blue", $offsetY="-40", $offsetX="-50")
      UpdateRelStyle(SystemC, customerA, $textColor="red", $lineColor="red", $offsetX="-50", $offsetY="20")

      UpdateLayoutConfig($c4ShapeInRow="3", $c4BoundaryInRow="1")
```
