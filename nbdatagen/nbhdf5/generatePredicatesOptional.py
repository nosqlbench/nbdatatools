import numpy as np
import torch
import h5py

def generate_knn_dataset(n, p, x, output_file, use_predicates):
  # Step 1: Generate 'train' data (n vectors of size p)
  train_data = np.random.rand(n, p).astype(np.float32)
  test_data = np.random.rand(x, p).astype(np.float32)

  if use_predicates:
    train_ids = np.repeat(np.arange(1, n // 100 + 1), 100)  # Assign 100 contiguous vectors per id
    test_ids = []
    for _ in range(x):
      num_ids = np.random.randint(1, 6)  # Each test query is associated with 1 to 5 training ids
      associated_ids = np.random.choice(np.arange(1, n // 100 + 1), size=num_ids, replace=False)
      test_ids.append(associated_ids)
  else:
    train_ids = None
    test_ids = None

  # Step 3: Compute KNN for 'test' data
  neighbors_list = []
  train_tensor = torch.tensor(train_data)
  for i in range(x):
    query_vector = torch.tensor(test_data[i])

    if use_predicates:
      query_ids = test_ids[i]
      mask = torch.tensor(np.isin(train_ids, query_ids))
      filtered_train_data = train_tensor[mask]
      global_indices = torch.where(mask)[0]  # Get global indices of the filtered train data
    else:
      filtered_train_data = train_tensor
      global_indices = torch.arange(n)

      if filtered_train_data.shape[0] > 0:
        # Compute Euclidean distance
        distances = torch.cdist(query_vector.unsqueeze(0), filtered_train_data).squeeze(0)

        # Get top 100 neighbors based on Euclidean distance (smallest values)
        knn_indices = torch.topk(distances, k=100, largest=False).indices
        global_knn_indices = global_indices[knn_indices]  # Map local indices to global indices
        neighbors_list.append(global_knn_indices.numpy())
      else:
        neighbors_list.append(np.full(100, -1, dtype=np.int32))  # Placeholder for no neighbors

  # Step 4: Write data to HDF5 file
  with h5py.File(output_file, 'w') as h5f:
    h5f.create_dataset('train', data=train_data)
    h5f.create_dataset('test', data=test_data)
    h5f.create_dataset('neighbors', data=np.array(neighbors_list, dtype=np.int32))

    if use_predicates:
      h5f.create_dataset('train_ids', data=train_ids)
      h5f.create_dataset('test_ids', data=np.array(test_ids, dtype=object), dtype=h5py.special_dtype(vlen=np.int32))

  print(f"Dataset saved to {output_file}")

# Example usage
if __name__ == "__main__":
  n = int(input("Enter the number of train vectors (n): "))
  p = int(input("Enter the dimensionality of each vector (p): "))
  x = int(input("Enter the number of test vectors (x): "))
  output_file = input("Enter the output HDF5 file name: ")
  use_predicates = input("Create dataset with predicates? (y/n): ").strip().lower() == 'y'

  generate_knn_dataset(n, p, x, output_file, use_predicates)
