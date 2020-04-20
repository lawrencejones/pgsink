select payload.*, from (
  select *, row_number() over (
    partition by
      
      
        
        payload.id
      
    order by timestamp desc
  ) as row_number
  from `project.dataset.example_raw`
)
where row_number = 1
and operation != 'DELETE'
