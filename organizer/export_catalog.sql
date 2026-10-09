-- Exporta el catálogo real (piezas, ubicaciones, listas de partes por modelo) y la
-- distribución de evaluaciones de la Lista de Partes, como un solo JSON.
-- Solo lectura. Uso (organizador, NO se reparte):
--   psql "$DATABASE_URL" -X -q -t -A -f organizer/export_catalog.sql > organizer/catalog.raw.json
begin transaction read only;

select json_build_object(
  'exported_at', now(),
  'units', (select json_agg(json_build_object('id', id, 'name', name) order by id) from piece_units),
  'pieces', (
    select json_agg(json_build_object(
      'id', p.id, 'code', p.code, 'name', p.name,
      'family', pc.name, 'group', pg.name, 'subgroup', ps.name,
      'unit_id', p.id_piece_unit
    ) order by p.id)
    from pieces p
    left join piece_classes   pc on pc.id = p.id_class
    left join piece_groups    pg on pg.id = p.id_group
    left join piece_subgroups ps on ps.id = p.id_subgroup
  ),
  'locations', (
    select json_agg(json_build_object(
      'id', id, 'name', name, 'barcode', barcode, 'is_workstation', is_workstation
    ) order by id)
    from locations
  ),
  'models', (
    select json_agg(json_build_object(
      'id', m.id, 'name', m.name, 'brand', b.name, 'equipment_type', et.name,
      'lines', (
        select json_agg(json_build_object(
          'id', mp.id, 'piece_id', mp.id_piece, 'name', mp.name,
          'group_name', mp.group_name, 'unit_id', mp.id_measurement_unit,
          'qty', mp.quantity_in_model, 'sort_order', mp.sort_order
        ) order by mp.sort_order, mp.id)
        from model_pieces mp where mp.id_model = m.id
      )
    ) order by m.id)
    from models m
    join brands b           on b.id  = m.id_brand
    join equipment_types et on et.id = b.id_equipment_type
    where exists (select 1 from model_pieces mp where mp.id_model = m.id)
  ),
  'evaluations', (
    select json_agg(json_build_object(
      'status', piece_initial_status, 'action', diagnostic_action, 'qty', quantity
    ))
    from entry_diagnostic_parts
    where piece_initial_status is not null
  )
);

rollback;
