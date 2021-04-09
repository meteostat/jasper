"""
Update normals inventory

The code is licensed under the MIT license.
"""

from routines import Routine

task = Routine('task.inventory.normals')

task.query('''
    INSERT INTO
        `inventory`(`station`, `mode`)
    SELECT
        `station`,
        'N' AS `mode`
    FROM
        `normals_global`
    GROUP BY
        `station`
''')
