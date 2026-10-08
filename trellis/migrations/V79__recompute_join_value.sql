-- Issue #944: a `recompute` ring row may carry a new image when it has no
-- prior image: a release's `staging::append::StagedChange::ReleasedJoinValue`,
-- a `{to_col: value}` object naming one join value the released key's parked
-- changes held. The fold reads a recompute's new image only into
-- `to_col_values` (`staging::fold`), never as an image, so the row stays an
-- image-less trigger. A recompute still never carries both a prior image and
-- a new one, and a truncate sentinel stays fully image-less.

alter table seg_0 drop constraint seg_0_recompute_has_no_images;
alter table seg_0 add constraint seg_0_recompute_has_no_images
    check (
        (op <> 'truncate' or (old_image is null and new_image is null))
        and (op <> 'recompute' or old_image is null or new_image is null)
    );

alter table seg_1 drop constraint seg_1_recompute_has_no_images;
alter table seg_1 add constraint seg_1_recompute_has_no_images
    check (
        (op <> 'truncate' or (old_image is null and new_image is null))
        and (op <> 'recompute' or old_image is null or new_image is null)
    );

alter table seg_2 drop constraint seg_2_recompute_has_no_images;
alter table seg_2 add constraint seg_2_recompute_has_no_images
    check (
        (op <> 'truncate' or (old_image is null and new_image is null))
        and (op <> 'recompute' or old_image is null or new_image is null)
    );

alter table seg_3 drop constraint seg_3_recompute_has_no_images;
alter table seg_3 add constraint seg_3_recompute_has_no_images
    check (
        (op <> 'truncate' or (old_image is null and new_image is null))
        and (op <> 'recompute' or old_image is null or new_image is null)
    );
