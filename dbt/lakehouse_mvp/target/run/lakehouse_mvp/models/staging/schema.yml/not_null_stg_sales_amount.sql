
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select amount
from "lakehouse"."staging_staging"."stg_sales"
where amount is null



  
  
      
    ) dbt_internal_test