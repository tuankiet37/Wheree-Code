# Content
This part using reviews of the brand to generated the content for website by `gpt-o4-mini`
- [`HOME.S1.P1.C1`](#home-s1-p1-c1)
- [`PHOTO.S20.P1.C1`](#photo-s20-p1-c1)
- [`MENU.S34.P1.C1`](#menu-s34-p1-c1)

The first two types are required for all brands, while `MENU.S34.P1.C1` is only included when the image of this brand has `MENU` type. 

# API Reference

| Parameter | Type     | Description                |
| :-------- | :------- | :------------------------- |
| `api_key` | `string` | API key |
| `base_url` | `string` | Link to third party service |

The default `base_url` is "https://open.keyai.shop/v1"

**Modify Paths**:
- Each file requires modification of the `source_path` *\(input file\)* and `base_dir` *\(directory to store success and failed output files\)*.


## `HOME.S1.P1.C1`
- The `HOME.S1.P1.C1`, along with the `category` and `advantages-disadvantages`, is generated in the same request in `Content_Main_Cat_Adv.ipynb`

- One request requires `orginalname` `fulladdress` `reviews` and `cate_lst`.

- **Output**: A structured dataset with the following two files:
  - File 1
    - `code`: Unique code identifier for each brand (UUID 4).
    - `content`: The generated content
    - `type`: `HOME.S1.P1.C1`
  - File 2
    - `code`: Unique code identifier for each brand (UUID 4).
    - `advantages`: Provide a two-sentence summary details the BRAND’s *positive* aspects.
    - `disadvantages`: rovide a two-sentence summary details the BRAND’s *negative* aspects.

## `PHOTO.S20.P1.C1`

- The `PHOTO.S20.P1.C1`, along with its `three subcategories` and `sub_score list`, is generated in the same request.

- One request requires `originalname`, `fulladdress`, `reviews`, `category`, `subcate_lst`, and `criteria_lst`.

- **Output**: A structured dataset with the following fields:
  - `code`: Unique code identifier for each brand (UUID 4).
  - `content`: The generated content. 
  - `type`: `PHOTO.S20.P1.C1`
  - `subcate1`: `sub_1`. This is the first subcategory associated with the content.
  - `subcate2`: `sub_2`. This is the second subcategory associated with the content.
  - `subcate3`: `sub_3`. This is the third subcategory associated with the content.
  - `sub_score`: `criteria_dict`. This field contains a dictionary of criteria scores related to the content. 

## `MENU.S34.P1.C1`
- One request requires `originalname`, `fulladdress` and `reviews`.

- **Output**: A structured dataset with the following fields:
  - `code`: Unique code identifier for each brand (UUID 4).
  - `content`: The generated content. 
  - `type`: `MENU.S34.P1.C1`






