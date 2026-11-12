# Hale Joli Solar 2.0 report

Source for the Power BI report "Hale Joli Solar 2.0" in the Connected Home workspace
(report id in `build/report_id.txt`), bound to the `HaleJoliSolar` semantic model.

- `build/gen.py` writes the PBIR definition (theme, six pages, visuals) to `report_new/`.
- `build/validate.py` checks every generated file against the PBIR schemas in `schemas/`.
- `build/deploy.py` pushes `report_new/` to the existing report and exports each page to PNG in `export_new/`.
- `report_cur/` holds the three base files copied from the original report (connection, version, base theme).

```bash
python3 -I build/gen.py && python3 -I build/validate.py && python3 -I build/deploy.py
```

Needs `az login` with access to the workspace. Colors are fixed per entity (solar amber,
home blue, battery green, grid import red, grid export violet) and were validated for
color-vision separation on the light surface.
