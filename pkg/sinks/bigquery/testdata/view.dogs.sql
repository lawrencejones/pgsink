select payload.*, from (
  select *, row_number() over (
    partition by
      
      
        
        payload.tag
      
    order by timestamp desc
  ) as row_number
  from `project.dataset.dogs_raw`
)
where row_number = 1
and operation != 'DELETE'
